package ports

import "context"

type EventHandler func(ctx context.Context, payload any)

type EventBus interface {
	Publish(ctx context.Context, topic string, payload any) error
	Subscribe(topic string, handler EventHandler) (unsubscribe func())
}
