package domain

import (
	"net/url"
)

type ServiceNode struct {
	Name      string
	Namespace string
}

func (s ServiceNode) ID() string {
	if s.Namespace == "" {
		return s.Name
	}
	return s.Namespace + "/" + s.Name
}

func (s ServiceNode) PathEscapeID() string {
	return url.PathEscape(s.ID())
}
