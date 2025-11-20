package domain

import "time"

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

type Alert struct {
	Service     ServiceNode
	Severity    Severity
	Risk        RiskScore
	Explanation string
	CreatedAt   time.Time
}
