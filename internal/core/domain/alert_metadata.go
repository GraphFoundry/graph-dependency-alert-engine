package domain

type AlertMetadata struct {
	RuleID         string
	RuleVersion    string
	ModelVersion   string
	FeatureSetHash string
}

type MitigationAction string

const (
	ActionThrottle MitigationAction = "throttle"
	ActionDegrade  MitigationAction = "degrade"
	ActionFailover MitigationAction = "failover"
	ActionObserve  MitigationAction = "observe"
	ActionScaleUp  MitigationAction = "scale_up"
)
