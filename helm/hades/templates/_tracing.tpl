{{/*
Resolve the OTLP endpoint spans are exported to: an explicit tracing.endpoint,
or the bundled in-cluster Jaeger when tracing.deployJaeger is set. Empty when
neither is configured.
*/}}
{{- define "hades.otelEndpoint" -}}
{{- if .Values.tracing.endpoint -}}
{{- .Values.tracing.endpoint -}}
{{- else if .Values.tracing.deployJaeger -}}
{{- printf "http://hades-jaeger.%s.svc.cluster.local:4317" .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{/*
Render the OTEL_EXPORTER_OTLP_ENDPOINT env entry when tracing is enabled and an
endpoint resolves. Include with `nindent 12` inside a container's env list.
*/}}
{{- define "hades.tracingEnv" -}}
{{- $ep := include "hades.otelEndpoint" . -}}
{{- if and .Values.tracing.enabled $ep }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ $ep | quote }}
{{- end }}
{{- end -}}
