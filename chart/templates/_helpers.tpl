{{/* vim: set filetype=mustache: */}}

{{/*
Return the Fastflow image name
*/}}
{{- define "fastflow.image" -}}
{{- if .Values.imageOverride -}}
{{- .Values.imageOverride -}}
{{- else -}}
{{ .Values.image.registry }}/{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end -}}
{{- end -}}
