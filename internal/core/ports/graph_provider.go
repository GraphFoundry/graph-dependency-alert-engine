package ports

import (
	"context"
	"graph-alert-engine/internal/core/domain"
)

type GraphProvider interface {
	GetServices(ctx context.Context) ([]domain.ServiceNode, error)
	GetCentrality(ctx context.Context) (map[string]domain.Centrality, error) // key = ServiceNode.ID()
	GetHealth(ctx context.Context) (domain.GraphHealth, error)
	GetPeers(ctx context.Context, svc domain.ServiceNode, direction domain.Direction, limit int) ([]domain.ServiceNode, error)
	GetNeighborhood(ctx context.Context, svc domain.ServiceNode, k int) ([]domain.ServiceNode, error)
}
