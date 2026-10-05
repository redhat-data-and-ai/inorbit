package fivetran

import "strings"

// ServiceName is the dashboard label for a Fivetran connector service id.
func ServiceName(service string) string {
	s := strings.ToLower(strings.TrimSpace(service))
	if s == "" {
		return ""
	}
	if n, ok := serviceNames[s]; ok {
		return n
	}
	s = strings.ReplaceAll(s, "_", " ")
	parts := strings.Fields(s)
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, " ")
}

var serviceNames = map[string]string{
	"google_sheets":      "Google Sheets",
	"google_drive":       "Google Drive",
	"google_ads":         "Google Ads",
	"google_analytics":   "Google Analytics",
	"amazon_s3":          "S3",
	"s3":                 "S3",
	"s3_compatible":      "S3",
	"salesforce":         "Salesforce",
	"salesforce_sandbox": "Salesforce",
	"postgres":           "Postgres",
	"postgres_rds":       "Postgres",
	"postgresql":         "Postgres",
	"aurora":             "Amazon Aurora",
	"aurora_postgres":    "Amazon Aurora PostgreSQL",
	"aurora_postgresql":  "Amazon Aurora PostgreSQL",
	"mysql":              "MySQL",
	"sql_server":         "SQL Server",
	"snowflake":          "Snowflake",
	"github":             "GitHub",
	"gitlab":             "GitLab",
	"jira":               "Jira",
	"jira_align":         "Jira",
	"zendesk":            "Zendesk",
	"zendesk_support":    "Zendesk",
	"hubspot":            "HubSpot",
	"marketo":            "Marketo",
	"stripe":             "Stripe",
	"shopify":            "Shopify",
	"workday":            "Workday",
	"netsuite":           "NetSuite",
	"oracle":             "Oracle",
	"mongodb":            "MongoDB",
	"kafka":              "Kafka",
	"kinesis":            "Kinesis",
	"gcs":                "GCS",
	"azure_blob_storage": "Azure Blob Storage",
	"sftp":               "SFTP",
	"ftp":                "FTP",
}
