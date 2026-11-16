{{/*
NATS connection env entries: the URL plus the credentials from the
nats.auth.secretName Secret. The bundled NATS server reads the same Secret
(nats.container.env), so rendering fails when the two names differ. Include
with `nindent 12` inside a container's env list.
*/}}
{{- define "hades.natsEnv" -}}
{{- $secret := .Values.nats.auth.secretName -}}
{{- if not $secret -}}
{{- fail "nats.auth.secretName is empty: set it to a Secret with keys NATS_USERNAME and NATS_PASSWORD (NATS does not run without authentication)" -}}
{{- end -}}
{{- range $key := list "NATS_USERNAME" "NATS_PASSWORD" -}}
{{- $server := dig $key "valueFrom" "secretKeyRef" "name" "" ($.Values.nats.container.env | default dict) -}}
{{- if ne $server $secret -}}
{{- fail (printf "nats.container.env.%s must read Secret %q (nats.auth.secretName), got %q: the NATS server and the Hades components need the same credentials" $key $secret $server) -}}
{{- end -}}
{{- end -}}
{{- if ne (dig "authorization" "password" "" (.Values.nats.config.merge | default dict)) "<< $NATS_PASSWORD >>" -}}
{{- fail "nats.config.merge.authorization was removed or changed: NATS must require the credentials from nats.auth.secretName" -}}
{{- end }}
- name: NATS_URL
  value: "nats://{{ .Values.nats.host }}:{{ .Values.nats.port }}"
- name: NATS_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ $secret | quote }}
      key: NATS_USERNAME
- name: NATS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $secret | quote }}
      key: NATS_PASSWORD
{{- end -}}
