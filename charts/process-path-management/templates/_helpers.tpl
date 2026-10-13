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
