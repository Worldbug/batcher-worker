package batchingworker

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
)

type ProcessFunc[T any] func(context.Context, []T)

func NewBatchingWorker[T any](
	process ProcessFunc[T],
	batchSize int,
	workersCount int,
	cleanDuration time.Duration,
) *BatchingWorker[T] {
	return &BatchingWorker[T]{
		batchSize:     batchSize,
		cleanDuration: cleanDuration,
		process:       process,
		items:         make(chan T),
		workersCount:  workersCount,
	}
}

// BatchingWorker Накапливает слайс T и запускает process
// если слайс достигает batchSize
// или проходит cleanDuration с момента последнего Send
type BatchingWorker[T any] struct {
	items         chan T
	process       ProcessFunc[T]
	batchSize     int
	workersCount  int
	cleanDuration time.Duration
}

func (w *BatchingWorker[T]) Start(ctx context.Context) error {
	eg := &errgroup.Group{}

	for i := 0; i < w.workersCount; i++ {
		eg.Go(func() error { return w.start(ctx) })
	}

	return eg.Wait()
}

func (w *BatchingWorker[T]) start(ctx context.Context) error {
	cleanTicker := time.NewTicker(w.cleanDuration)
	defer cleanTicker.Stop()

	items := make([]T, 0, w.batchSize)

	for {
		select {
		case <-cleanTicker.C:
			if len(items) == 0 {
				continue
			}

			w.process(ctx, items)
			items = make([]T, 0, w.batchSize)

		case item := <-w.items:
			cleanTicker.Reset(w.cleanDuration)

			items = append(items, item)
			if len(items) == w.batchSize {
				w.process(ctx, items)
				items = make([]T, 0, w.batchSize)
			}

		case <-ctx.Done():
			if len(items) == 0 {
				return ctx.Err()
			}

			w.process(context.Background(), items)
			return ctx.Err()
		}
	}
}

func (w *BatchingWorker[T]) Send(ctx context.Context, item T) {
	w.items <- item
}
