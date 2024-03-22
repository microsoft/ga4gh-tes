//go:build !custom || outputs || outputs.azure_append_blob

package all

import _ "github.com/influxdata/telegraf/plugins/outputs/azure_append_blob" // register plugin
