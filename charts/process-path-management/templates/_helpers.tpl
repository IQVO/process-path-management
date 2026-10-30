{{/*
Expand the name of the chart.
*/}}
{{- define "process-path-management.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "process-path-management.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart name and version as used by the chart label.
*/}}
{{- define "process-path-management.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "process-path-management.labels" -}}
helm.sh/chart: {{ include "process-path-management.chart" . }}
{{ include "process-path-management.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "process-path-management.selectorLabels" -}}
app.kubernetes.io/name: {{ include "process-path-management.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "process-path-management.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "process-path-management.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding DATABASE_URL, when the chart creates its own.
*/}}
{{- define "process-path-management.databaseSecretName" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecret }}
{{- else }}
{{- include "process-path-management.fullname" . }}-database
{{- end }}
{{- end }}

{{/*
Fully qualified name of the MCP server deployment/service.
*/}}
{{- define "process-path-management.mcpFullname" -}}
{{- include "process-path-management.fullname" . }}-mcp
{{- end }}

{{/*
Fully qualified name of the A2A agent Deployment/Service (<release>-a2a, ADR 0039).
*/}}
{{- define "process-path-management.a2aFullname" -}}
{{- include "process-path-management.fullname" . }}-a2a
{{- end }}

{{/*
MANDATORY (ADR 0037: Postgres is the only runtime). cmd/pathmgmt and cmd/mcp exit
at boot without DATABASE_URL; there is no in-memory mode.
*/}}
{{- define "process-path-management.requireDatabase" -}}
{{- if not (or .Values.database.url .Values.database.existingSecret) -}}
{{- fail "process-path-management requires database.url or database.existingSecret to be set (DATABASE_URL): the service runs on real Postgres only and exits at boot without it (ADR 0037)." -}}
{{- end -}}
{{- end -}}

{{/*
MANDATORY (ADR 0037: Kafka is the only runtime). cmd/pathmgmt and
cmd/pathmgmt-projector exit at boot without KAFKA_BROKERS, and cmd/pathmgmt
rejects EVENT_PUBLISHER=log; there is no default broker, no log sink and no way
to switch Kafka off. kafka.enabled is kept only so values written before ADR
0037 still render (warehouse-infra sets it to 'true'): it may be true, never
false. This service has no inbound Kafka consumer, so there is no consumer
group to require.
*/}}
{{- define "process-path-management.requireKafka" -}}
{{- $_ := required "process-path-management requires kafka.brokers to be set (KAFKA_BROKERS, comma-separated): the service runs on real Kafka only and exits at boot without it (ADR 0037)." .Values.kafka.brokers -}}
{{- if and (hasKey .Values.kafka "enabled") (or (not .Values.kafka.enabled) (eq (toString .Values.kafka.enabled) "false")) -}}
{{- fail "kafka.enabled is false — Kafka cannot be switched off: the service runs on real Kafka only (ADR 0037). Drop the key or set it to true and set kafka.brokers." -}}
{{- end -}}
{{- if ne (toString .Values.config.eventPublisher) "kafka" -}}
{{- fail (printf "config.eventPublisher is %q — the only publisher is \"kafka\": the log sink was removed (ADR 0037); the binary rejects EVENT_PUBLISHER=log at boot." (toString .Values.config.eventPublisher)) -}}
{{- end -}}
{{- end -}}

{{/*
Shutdown is sequential: readiness flips, then ONE 10s budget drains HTTP and
lets the outbox relay finish, then a 5s telemetry flush. A grace period under
20s gets the pod SIGKILLed mid-drain (the default is 30s).
*/}}
{{- define "process-path-management.requireGracePeriod" -}}
{{- if lt (int .Values.terminationGracePeriodSeconds) 20 -}}
{{- fail (printf "terminationGracePeriodSeconds is %v — want at least 20: 10s HTTP/relay drain (one shared budget) + 5s telemetry flush." .Values.terminationGracePeriodSeconds) -}}
{{- end -}}
{{- end -}}

{{/*
extraEnv may not bring back an in-memory or log mode (ADR 0037): no
EVENT_PUBLISHER other than kafka and no *_MODE variable at all. This context
reads no *_MODE switch (it consumes no sibling context over REST or Kafka), so
any one in extraEnv is a leftover of a permissive/off mode that no longer exists.
*/}}
{{- define "process-path-management.requireNoInMemoryEnv" -}}
{{- range .Values.extraEnv -}}
{{- $name := toString .name -}}
{{- $value := toString (default "" .value) -}}
{{- if and (eq $name "EVENT_PUBLISHER") (ne $value "kafka") -}}
{{- fail (printf "extraEnv sets EVENT_PUBLISHER=%s — the only publisher is \"kafka\" (ADR 0037)." $value) -}}
{{- end -}}
{{- if hasSuffix "_MODE" $name -}}
{{- fail (printf "extraEnv sets %s — *_MODE variables are not configurable: process-path-management runs on real Postgres and real Kafka only, with no permissive or off mode (ADR 0037)." $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Every render-time requirement of the real runtime, in one include. The api
Deployment (always rendered) includes it, so a mis-set release fails
`helm template` / `helm install` whichever component is enabled.
*/}}
{{- define "process-path-management.requireRealRuntime" -}}
{{- include "process-path-management.requireDatabase" . -}}
{{- include "process-path-management.requireKafka" . -}}
{{- include "process-path-management.requireGracePeriod" . -}}
{{- include "process-path-management.requireNoInMemoryEnv" . -}}
{{- end -}}

{{/*
Fully qualified name of the analytics projector deployment (ADR 0007).
*/}}
{{- define "process-path-management.projectorFullname" -}}
{{- include "process-path-management.fullname" . }}-projector
{{- end }}

{{/*
Fully qualified name of the analytics reports deployment/service (ADR 0007).
*/}}
{{- define "process-path-management.reportsFullname" -}}
{{- include "process-path-management.fullname" . }}-reports
{{- end }}

{{/*
Name of the Secret holding the analytics DSNs, when the chart creates its own.
*/}}
{{- define "process-path-management.analyticsSecretName" -}}
{{- if .Values.analytics.database.existingSecret }}
{{- .Values.analytics.database.existingSecret }}
{{- else }}
{{- include "process-path-management.fullname" . }}-analytics
{{- end }}
{{- end }}

{{/*
Fully qualified name of the frontend Module Federation remote deployment/service.

The remote is served by its own nginx pod and reached through warehouse-infra's
Nginx web gateway at /mfes/process-path-management/. It is deliberately a separate
workload from the API: Kong never routes to it, and the OLTP Service must never
select it.
*/}}
{{- define "process-path-management.frontendFullname" -}}
{{- include "process-path-management.fullname" . }}-frontend
{{- end }}

{{/*
Comma-joined names of this chart's enabled KongPlugins, for the
konghq.com/plugins annotation on every HTTPRoute this chart renders.
Empty string (annotation omitted) when none are enabled — standalone
installs without Kong are unaffected.
*/}}
{{- define "process-path-management.kongPluginNames" -}}
{{- $names := list -}}
{{- range $name, $cfg := .Values.kongPlugins -}}
{{- if $cfg.enabled -}}
{{- $names = append $names (printf "%s-%s" (include "process-path-management.fullname" $) $name) -}}
{{- end -}}
{{- end -}}
{{- join "," $names -}}
{{- end }}
