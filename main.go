package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Country code to name mapping
var countryCodeToName = map[string]string{
	"AD": "Andorra", "AE": "United Arab Emirates", "AF": "Afghanistan", "AG": "Antigua and Barbuda", "AI": "Anguilla", "AL": "Albania", "AM": "Armenia", "AO": "Angola", "AQ": "Antarctica", "AR": "Argentina", "AS": "American Samoa", "AT": "Austria", "AU": "Australia", "AW": "Aruba", "AX": "Åland Islands", "AZ": "Azerbaijan", "BA": "Bosnia and Herzegovina", "BB": "Barbados", "BD": "Bangladesh", "BE": "Belgium", "BF": "Burkina Faso", "BG": "Bulgaria", "BH": "Bahrain", "BI": "Burundi", "BJ": "Benin", "BL": "Saint Barthélemy", "BM": "Bermuda", "BN": "Brunei", "BO": "Bolivia", "BQ": "Caribbean Netherlands", "BR": "Brazil", "BS": "Bahamas", "BT": "Bhutan", "BV": "Bouvet Island", "BW": "Botswana", "BY": "Belarus", "BZ": "Belize", "CA": "Canada", "CC": "Cocos (Keeling) Islands", "CD": "Congo (DRC)", "CF": "Central African Republic", "CG": "Congo (Republic)", "CH": "Switzerland", "CI": "Côte d'Ivoire", "CK": "Cook Islands", "CL": "Chile", "CM": "Cameroon", "CN": "China", "CO": "Colombia", "CR": "Costa Rica", "CU": "Cuba", "CV": "Cabo Verde", "CW": "Curaçao", "CX": "Christmas Island", "CY": "Cyprus", "CZ": "Czechia", "DE": "Germany", "DJ": "Djibouti", "DK": "Denmark", "DM": "Dominica", "DO": "Dominican Republic", "DZ": "Algeria", "EC": "Ecuador", "EE": "Estonia", "EG": "Egypt", "EH": "Western Sahara", "ER": "Eritrea", "ES": "Spain", "ET": "Ethiopia", "FI": "Finland", "FJ": "Fiji", "FM": "Micronesia", "FO": "Faroe Islands", "FR": "France", "GA": "Gabon", "GB": "United Kingdom", "GD": "Grenada", "GE": "Georgia", "GF": "French Guiana", "GG": "Guernsey", "GH": "Ghana", "GI": "Gibraltar", "GL": "Greenland", "GM": "Gambia", "GN": "Guinea", "GP": "Guadeloupe", "GQ": "Equatorial Guinea", "GR": "Greece", "GT": "Guatemala", "GU": "Guam", "GW": "Guinea-Bissau", "GY": "Guyana", "HK": "Hong Kong", "HM": "Heard Island and McDonald Islands", "HN": "Honduras", "HR": "Croatia", "HT": "Haiti", "HU": "Hungary", "ID": "Indonesia", "IE": "Ireland", "IL": "Israel", "IM": "Isle of Man", "IN": "India", "IO": "British Indian Ocean Territory", "IQ": "Iraq", "IR": "Iran", "IS": "Iceland", "IT": "Italy", "JE": "Jersey", "JM": "Jamaica", "JO": "Jordan", "JP": "Japan", "KE": "Kenya", "KG": "Kyrgyzstan", "KH": "Cambodia", "KI": "Kiribati", "KM": "Comoros", "KN": "Saint Kitts and Nevis", "KP": "North Korea", "KR": "South Korea", "KW": "Kuwait", "KY": "Cayman Islands", "KZ": "Kazakhstan", "LA": "Laos", "LB": "Lebanon", "LC": "Saint Lucia", "LI": "Liechtenstein", "LK": "Sri Lanka", "LR": "Liberia", "LS": "Lesotho", "LT": "Lithuania", "LU": "Luxembourg", "LV": "Latvia", "LY": "Libya", "MA": "Morocco", "MC": "Monaco", "MD": "Moldova", "ME": "Montenegro", "MF": "Saint Martin", "MG": "Madagascar", "MH": "Marshall Islands", "MK": "North Macedonia", "ML": "Mali", "MM": "Myanmar", "MN": "Mongolia", "MO": "Macao", "MP": "Northern Mariana Islands", "MQ": "Martinique", "MR": "Mauritania", "MS": "Montserrat", "MT": "Malta", "MU": "Mauritius", "MV": "Maldives", "MW": "Malawi", "MX": "Mexico", "MY": "Malaysia", "MZ": "Mozambique", "NA": "Namibia", "NC": "New Caledonia", "NE": "Niger", "NF": "Norfolk Island", "NG": "Nigeria", "NI": "Nicaragua", "NL": "Netherlands", "NO": "Norway", "NP": "Nepal", "NR": "Nauru", "NU": "Niue", "NZ": "New Zealand", "OM": "Oman", "PA": "Panama", "PE": "Peru", "PF": "French Polynesia", "PG": "Papua New Guinea", "PH": "Philippines", "PK": "Pakistan", "PL": "Poland", "PM": "Saint Pierre and Miquelon", "PN": "Pitcairn Islands", "PR": "Puerto Rico", "PT": "Portugal", "PW": "Palau", "PY": "Paraguay", "QA": "Qatar", "RE": "Réunion", "RO": "Romania", "RS": "Serbia", "RU": "Russia", "RW": "Rwanda", "SA": "Saudi Arabia", "SB": "Solomon Islands", "SC": "Seychelles", "SD": "Sudan", "SE": "Sweden", "SG": "Singapore", "SH": "Saint Helena", "SI": "Slovenia", "SJ": "Svalbard and Jan Mayen", "SK": "Slovakia", "SL": "Sierra Leone", "SM": "San Marino", "SN": "Senegal", "SO": "Somalia", "SR": "Suriname", "SS": "South Sudan", "ST": "São Tomé and Príncipe", "SV": "El Salvador", "SX": "Sint Maarten", "SY": "Syria", "SZ": "Eswatini", "TC": "Turks and Caicos Islands", "TD": "Chad", "TF": "French Southern Territories", "TG": "Togo", "TH": "Thailand", "TJ": "Tajikistan", "TK": "Tokelau", "TL": "Timor-Leste", "TM": "Turkmenistan", "TN": "Tunisia", "TO": "Tonga", "TR": "Turkey", "TT": "Trinidad and Tobago", "TV": "Tuvalu", "TZ": "Tanzania", "UA": "Ukraine", "UG": "Uganda", "UM": "U.S. Outlying Islands", "US": "United States", "UY": "Uruguay", "UZ": "Uzbekistan", "VA": "Vatican City", "VC": "Saint Vincent and the Grenadines", "VE": "Venezuela", "VG": "British Virgin Islands", "VI": "U.S. Virgin Islands", "VN": "Vietnam", "VU": "Vanuatu", "WF": "Wallis and Futuna", "WS": "Samoa", "YE": "Yemen", "YT": "Mayotte", "ZA": "South Africa", "ZM": "Zambia", "ZW": "Zimbabwe",
}

// Structs
type GraphQLRequest struct {
	Query string `json:"query"`
}
type ZoneDetailsResponse struct {
	Result struct {
		Name string `json:"name"`
	} `json:"result"`
	Success bool `json:"success"`
}
type RangeData struct {
	Requests            float64 `json:"requests"`
	Bytes               float64 `json:"bytes"`
	PageViews           float64 `json:"pageViews"`
	UniqueVisitors      float64 `json:"uniqueVisitors"`
	CachedRequests      float64 `json:"cachedRequests"`
	CachedBytes         float64 `json:"cachedBytes"`
	Threats             float64 `json:"threats"`
	CacheRate           float64 `json:"cacheRate"`
	ErrorRate           float64 `json:"errorRate"`
	CachedBandwidthRate float64 `json:"cachedBandwidthRate"`
}
type ZoneMetrics struct {
	Zone  string    `json:"zone"`
	Day1  RangeData `json:"day1"`
	Day7  RangeData `json:"day7"`
	Day30 RangeData `json:"day30"`
}
type countryData struct {
	name     string
	requests float64
	bytes    float64
}

// Helper Functions
func calcRates(rd *RangeData, statusMap []map[string]interface{}) {
	if rd.Requests > 0 {
		rd.CacheRate = (rd.CachedRequests / rd.Requests) * 100
	}
	if rd.Bytes > 0 {
		rd.CachedBandwidthRate = (rd.CachedBytes / rd.Bytes) * 100
	}
	if statusMap != nil && rd.Requests > 0 {
		var errReqs float64
		rd.ErrorRate = 0
		for _, item := range statusMap {
			status := asString(item, "key")
			count := getFloat(item, "requests")
			if strings.HasPrefix(status, "4") || strings.HasPrefix(status, "5") {
				errReqs += count
			}
		}
		if errReqs > 0 {
			rd.ErrorRate = (errReqs / rd.Requests) * 100
		}
	}
}
func safeGetMap(v interface{}) (map[string]interface{}, bool) {
	if v == nil {
		return nil, false
	}
	m, ok := v.(map[string]interface{})
	return m, ok
}
func safeGetArray(v interface{}) ([]interface{}, bool) {
	if v == nil {
		return nil, false
	}
	arr, ok := v.([]interface{})
	return arr, ok
}
func getFloat(m map[string]interface{}, key string) float64 {
	if v, ok := m[key]; ok {
		if f, ok := v.(float64); ok {
			return f
		}
	}
	return 0
}
func asString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// Prometheus Metrics
var (
	graphqlErrorsTotal       = promauto.NewCounter(prometheus.CounterOpts{Name: "cf_exporter_graphql_errors_total", Help: "Total number of failed GraphQL API queries."})
	cloudflareScrapeDuration = promauto.NewSummary(prometheus.SummaryOpts{Name: "cf_exporter_cloudflare_scrape_duration_seconds", Help: "Duration of the data scrape from the Cloudflare API."})
	exporterScrapeSuccess    = promauto.NewGauge(prometheus.GaugeOpts{Name: "cf_exporter_last_scrape_success", Help: "Set to 1 if the last scrape of Cloudflare data was successful, 0 otherwise."})
	accountVisitsGauge       = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_account_visits_total", Help: "Total visits for the entire account"}, []string{"account_id", "period"})
	reqGauge                 = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_requests_total", Help: "Total requests per zone"}, []string{"zone", "period"})
	bandwidthGauge           = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_bandwidth_bytes_total", Help: "Total bandwidth in bytes per zone"}, []string{"zone", "period"})
	pageViewsGauge           = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_pageviews_total", Help: "Total page views per zone"}, []string{"zone", "period"})
	zoneUniqueVisitorsGauge  = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_zone_unique_visitors_total", Help: "Total unique visitors per zone"}, []string{"zone", "period"})
	threatsGauge             = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_threats_total", Help: "Total threats per zone"}, []string{"zone", "period"})
	cacheRateGauge           = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_cache_rate_percent", Help: "Cache request rate per zone"}, []string{"zone", "period"})
	errorRateGauge           = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_error_rate_percent", Help: "Error rate (4xx+5xx) per zone"}, []string{"zone", "period"})
	cachedBandwidthRateGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_cached_bandwidth_rate_percent", Help: "Cached bandwidth rate per zone"}, []string{"zone", "period"})
	errors4xxGauge           = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_errors_4xx_total", Help: "Total 4xx errors per zone"}, []string{"zone", "period"})
	errors5xxGauge           = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_errors_5xx_total", Help: "Total 5xx errors per zone"}, []string{"zone", "period"})
	countryReqGauge          = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_country_requests_total", Help: "Requests per country (rolling 24h, all zones)"}, []string{"country_name", "country_code"})
	countryBandwidthGauge    = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_country_bandwidth_bytes_total", Help: "Bandwidth per country (rolling 24h, all zones)"}, []string{"country_name", "country_code"})
	httpVersionGauge         = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_http_version_requests_total", Help: "Requests by client HTTP version per zone"}, []string{"zone", "period", "version"})
	sslProtocolGauge         = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_ssl_protocol_requests_total", Help: "Requests by client SSL/TLS protocol per zone"}, []string{"zone", "period", "protocol"})
	browserGauge             = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_browser_pageviews_total", Help: "Page views by browser family per zone"}, []string{"zone", "period", "browser"})
	ipClassGauge             = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_ip_class_requests_total", Help: "Requests by IP class per zone"}, []string{"zone", "period", "class"})
	threatPathGauge          = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_threat_path_requests_total", Help: "Requests by threat pathing name per zone"}, []string{"zone", "period", "path"})
	encryptedRequestsGauge   = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_encrypted_requests_total", Help: "Total encrypted requests for the account"}, []string{"account_id", "period"})
	encryptedBytesGauge      = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_encrypted_bytes_total", Help: "Total encrypted bandwidth for the account"}, []string{"account_id", "period"})
	contentTypeRequestsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_content_type_requests_total", Help: "Requests by content type per zone"}, []string{"zone", "period", "content_type"})
	contentTypeBytesGauge    = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "cf_content_type_bytes_total", Help: "Bandwidth by content type per zone"}, []string{"zone", "period", "content_type"})
)

// Query Builders
func buildAccountVisitsQuery(accountID, since, until string) string {
	return fmt.Sprintf(`query { viewer { accounts(filter: {accountTag: "%s"}) {
	requests: httpRequestsOverviewAdaptiveGroups(limit: 1, filter: {datetime_geq: "%s", datetime_lt: "%s"}) { sum { visits } }
	}}}`, accountID, since, until)
}

func buildSecurityQuery(accountID, since, until string) string {
	return fmt.Sprintf(`query { viewer { accounts(filter: {accountTag: "%s"}) {
	encrypted: httpRequestsOverviewAdaptiveGroups(limit: 1, filter: {datetime_geq: "%s", datetime_lt: "%s", clientSSLProtocol_neq: "none"}) { sum { requests, bytes } }
	}}}`, accountID, since, until)
}

func buildZoneCoreQuery(zoneIDs []string, since1, until1, since7, until7, since30, until30 string) string {
	q := "query { viewer {\n"
	for i, id := range zoneIDs {
		alias := fmt.Sprintf("z%d", i+1)
		q += fmt.Sprintf(` 	%s: zones(filter: { zoneTag: "%s" }) {
			day1: httpRequests1dGroups(limit: 1, filter: { date_geq: "%s", date_lt: "%s" }) { sum { requests bytes pageViews cachedRequests cachedBytes threats } uniq { uniques } }
			day7: httpRequests1dGroups(limit: 1, filter: { date_geq: "%s", date_lt: "%s" }) { sum { requests bytes pageViews cachedRequests cachedBytes threats } uniq { uniques } }
			day30: httpRequests1dGroups(limit: 1, filter: { date_geq: "%s", date_lt: "%s" }) { sum { requests bytes pageViews cachedRequests cachedBytes threats } uniq { uniques } }
		}
`, alias, id, since1, until1, since7, until7, since30, until30)
	}
	q += "} }"
	return q
}

func buildBreakdownsQuery(zoneIDs []string, since, until string) string {
	q := "query { viewer {\n"
	for i, id := range zoneIDs {
		alias := fmt.Sprintf("z%d", i+1)
		q += fmt.Sprintf(` 	%s: zones(filter: {zoneTag: "%s"}) {
			data: httpRequests1dGroups(limit: 1, filter: {date_geq: "%s", date_lt: "%s"}) {
				sum {
					browserMap { key: uaBrowserFamily, pageViews }
					clientSSLMap { key: clientSSLProtocol, requests }
					responseStatusMap { key: edgeResponseStatus, requests }
					contentTypeMap { requests, bytes, key: edgeResponseContentTypeName }
					ipClassMap { requests, key: ipType }
					threatPathingMap { requests, key: threatPathingName }
				}
			}
		}
`, alias, id, since, until)
	}
	q += "} }"
	return q
}

func buildHttpVersionQuery(zoneIDs []string, since, until string) string {
	q := "query { viewer {\n"
	for i, id := range zoneIDs {
		alias := fmt.Sprintf("z%d", i+1)
		q += fmt.Sprintf(` 	%s: zones(filter: {zoneTag: "%s"}) {
			httpRequestsAdaptiveGroups(limit: 10, filter: {datetime_geq: "%s", datetime_lt: "%s"}) {
				count
				dimensions { clientRequestHTTPProtocol }
			}
		}
`, alias, id, since, until)
	}
	q += "} }"
	return q
}

func buildTopCountriesQuery(zoneIDs []string, since, until time.Time) string {
	q := "query { viewer {\n"
	for i, id := range zoneIDs {
		alias := fmt.Sprintf("z%d", i+1)
		q += fmt.Sprintf(` 	%s: zones(filter: { zoneTag: "%s" }) {
			topCountries: httpRequestsAdaptiveGroups(limit: 250, filter: { datetime_geq: "%s", datetime_lt: "%s" }) {
				count
				dimensions { clientCountryName }
				sum { bytes: edgeResponseBytes }
			}
		}
`, alias, id, since.UTC().Format("2006-01-02T15:04:05Z"), until.UTC().Format("2006-01-02T15:04:05Z"))
	}
	q += "} }"
	return q
}

// Main Function
func main() {
	file, err := os.OpenFile("exporter.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer file.Close()
	mw := io.MultiWriter(os.Stdout, file)
	log.SetOutput(mw)

	apiToken := os.Getenv("CF_API_TOKEN")
	if apiToken == "" {
		log.Fatal("CF_API_TOKEN is required")
	}
	accountID := os.Getenv("CF_ACCOUNT_ID")
	if accountID == "" {
		log.Fatal("CF_ACCOUNT_ID is required")
	}
	zoneEnv := os.Getenv("CF_ZONE_IDS")
	if zoneEnv == "" {
		log.Fatal("CF_ZONE_IDS is required")
	}
	zoneIDs := strings.Split(zoneEnv, ",")
	addr := os.Getenv("EXPORTER_ADDR")
	if addr == "" {
		addr = ":2112"
	}
	refreshMinutes := 5
	if v := os.Getenv("REFRESH_MINUTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			refreshMinutes = n
		}
	}
	tz := os.Getenv("LOCAL_TZ")
	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		log.Fatalf("failed to load timezone %q: %v", tz, err)
	}

	client := &http.Client{Timeout: 60 * time.Second}

	callGQL := func(query string) (map[string]interface{}, error) {
		payload, _ := json.Marshal(GraphQLRequest{Query: query})
		log.Printf("DEBUG: Sending GraphQL request...")
		req, err := http.NewRequest("POST", "https://api.cloudflare.com/client/v4/graphql", bytes.NewBuffer(payload))
		if err != nil {
			graphqlErrorsTotal.Inc()
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiToken)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			graphqlErrorsTotal.Inc()
			return nil, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			graphqlErrorsTotal.Inc()
			return nil, err
		}
		var out map[string]interface{}
		if err := json.Unmarshal(body, &out); err != nil {
			log.Printf("ERROR unmarshalling JSON: %s", string(body))
			graphqlErrorsTotal.Inc()
			return nil, err
		}
		if errs, ok := out["errors"]; ok && errs != nil {
			log.Printf("WARN: GraphQL query returned errors: %v", errs)
			// Do not increment graphqlErrorsTotal here for authz errors, as they are expected for some plans
		} else {
			log.Printf("DEBUG: GraphQL request successful.")
		}
		return out, nil
	}

	update := func() {
		startTime := time.Now()
		defer func() {
			duration := time.Since(startTime).Seconds()
			cloudflareScrapeDuration.Observe(duration)
		}()
		exporterScrapeSuccess.Set(0)

		zoneIDToNameMap := make(map[string]string)
		log.Printf("Fetching zone names via REST API...")
		for _, id := range zoneIDs {
			trimmedID := strings.TrimSpace(id)
			if trimmedID == "" {
				continue
			}
			url := fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s", trimmedID)
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				log.Printf("WARN: Failed to create request for zone %s: %v", trimmedID, err)
				continue
			}
			req.Header.Set("Authorization", "Bearer "+apiToken)
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("WARN: Failed to fetch details for zone %s: %v", trimmedID, err)
				continue
			}

			var zoneDetails ZoneDetailsResponse
			err = json.NewDecoder(resp.Body).Decode(&zoneDetails)
			resp.Body.Close()
			if err != nil {
				log.Printf("WARN: Failed to unmarshal JSON for zone %s: %v", trimmedID, err)
				continue
			}

			if zoneDetails.Success && zoneDetails.Result.Name != "" {
				zoneIDToNameMap[trimmedID] = zoneDetails.Result.Name
			} else {
				log.Printf("WARN: API call for zone %s was not successful or name was empty.", trimmedID)
				zoneIDToNameMap[trimmedID] = trimmedID
			}
		}
		log.Printf("DEBUG: Mapped Zone IDs to Names: %v", zoneIDToNameMap)

		nowInLoc := time.Now().In(loc)
		untilDay := nowInLoc.Truncate(24 * time.Hour)
		since1d := untilDay.AddDate(0, 0, -1)
		since7d := untilDay.AddDate(0, 0, -7)
		since30d := untilDay.AddDate(0, 0, -30)

		dateRanges := map[string]map[string]string{
			"day1":  {"since": since1d.Format("2006-01-02"), "until": untilDay.Format("2006-01-02")},
			"day7":  {"since": since7d.Format("2006-01-02"), "until": untilDay.Format("2006-01-02")},
			"day30": {"since": since30d.Format("2006-01-02"), "until": untilDay.Format("2006-01-02")},
		}
		timeRanges := map[string]time.Time{
			"day1":  since1d,
			"day7":  since7d,
			"day30": since30d,
		}

		for period, since := range timeRanges {
			gqlAccount, _ := callGQL(buildAccountVisitsQuery(accountID, since.Format("2006-01-02T15:04:05Z"), nowInLoc.Format("2006-01-02T15:04:05Z")))
			if data, ok := safeGetMap(gqlAccount["data"]); ok {
				if viewer, ok := safeGetMap(data["viewer"]); ok {
					if accounts, ok := safeGetArray(viewer["accounts"]); ok && len(accounts) > 0 {
						if accountObj, ok := safeGetMap(accounts[0]); ok {
							if requestsArr, ok := safeGetArray(accountObj["requests"]); ok && len(requestsArr) > 0 {
								if requestsObj, ok := safeGetMap(requestsArr[0]); ok {
									if sum, ok := safeGetMap(requestsObj["sum"]); ok {
										accountVisitsGauge.WithLabelValues(accountID, period).Set(getFloat(sum, "visits"))
									}
								}
							}
						}
					}
				}
			}
			gqlSecurity, _ := callGQL(buildSecurityQuery(accountID, since.Format("2006-01-02T15:04:05Z"), nowInLoc.Format("2006-01-02T15:04:05Z")))
			if data, ok := safeGetMap(gqlSecurity["data"]); ok {
				if viewer, ok := safeGetMap(data["viewer"]); ok {
					if accounts, ok := safeGetArray(viewer["accounts"]); ok && len(accounts) > 0 {
						if accountObj, ok := safeGetMap(accounts[0]); ok {
							if encryptedArr, ok := safeGetArray(accountObj["encrypted"]); ok && len(encryptedArr) > 0 {
								if encryptedObj, ok := safeGetMap(encryptedArr[0]); ok {
									if sum, ok := safeGetMap(encryptedObj["sum"]); ok {
										encryptedRequestsGauge.WithLabelValues(accountID, period).Set(getFloat(sum, "requests"))
										encryptedBytesGauge.WithLabelValues(accountID, period).Set(getFloat(sum, "bytes"))
									}
								}
							}
						}
					}
				}
			}
		}

		gqlCore, _ := callGQL(buildZoneCoreQuery(zoneIDs, dateRanges["day1"]["since"], dateRanges["day1"]["until"], dateRanges["day7"]["since"], dateRanges["day7"]["until"], dateRanges["day30"]["since"], dateRanges["day30"]["until"]))
		results := make([]ZoneMetrics, len(zoneIDs))
		if data, ok := safeGetMap(gqlCore["data"]); ok {
			if viewerCore, ok := safeGetMap(data["viewer"]); ok {
				for i, id := range zoneIDs {
					trimmedID := strings.TrimSpace(id)
					if name, ok := zoneIDToNameMap[trimmedID]; ok {
						results[i].Zone = name
					} else {
						results[i].Zone = trimmedID
					}
					alias := fmt.Sprintf("z%d", i+1)
					if zonesArr, ok := safeGetArray(viewerCore[alias]); ok && len(zonesArr) > 0 {
						if zoneObj, ok := safeGetMap(zonesArr[0]); ok {
							extract := func(periodName string) RangeData {
								if periodData, ok := safeGetArray(zoneObj[periodName]); ok && len(periodData) > 0 {
									if periodObj, ok := safeGetMap(periodData[0]); ok {
										sum, _ := safeGetMap(periodObj["sum"])
										uniq, _ := safeGetMap(periodObj["uniq"])
										return RangeData{
											Requests:       getFloat(sum, "requests"),
											Bytes:          getFloat(sum, "bytes"),
											PageViews:      getFloat(sum, "pageViews"),
											CachedRequests: getFloat(sum, "cachedRequests"),
											CachedBytes:    getFloat(sum, "cachedBytes"),
											Threats:        getFloat(sum, "threats"),
											UniqueVisitors: getFloat(uniq, "uniques"),
										}
									}
								}
								return RangeData{}
							}
							results[i].Day1, results[i].Day7, results[i].Day30 = extract("day1"), extract("day7"), extract("day30")
						}
					}
				}
			}
		}

		// Reset vectors before populating
		ipClassGauge.Reset()
		threatPathGauge.Reset()

		for period, dates := range dateRanges {
			gqlBreakdown, _ := callGQL(buildBreakdownsQuery(zoneIDs, dates["since"], dates["until"]))
			if gqlBreakdown != nil {
				if data, ok := safeGetMap(gqlBreakdown["data"]); ok {
					if viewerBreakdown, ok := safeGetMap(data["viewer"]); ok {
						for i := range zoneIDs {
							alias := fmt.Sprintf("z%d", i+1)
							if zonesArr, ok := safeGetArray(viewerBreakdown[alias]); ok && len(zonesArr) > 0 {
								if zoneObj, ok := safeGetMap(zonesArr[0]); ok {
									if dayData, ok := safeGetArray(zoneObj["data"]); ok && len(dayData) > 0 {
										if dayObj, ok := safeGetMap(dayData[0]); ok {
											if sum, ok := safeGetMap(dayObj["sum"]); ok {
												statusMap, _ := safeGetArray(sum["responseStatusMap"])
												var statusMapForCalc []map[string]interface{}
												var err4xx, err5xx float64
												for _, item := range statusMap {
													s, _ := safeGetMap(item)
													statusMapForCalc = append(statusMapForCalc, s)
													if strings.HasPrefix(asString(s, "key"), "4") {
														err4xx += getFloat(s, "requests")
													}
													if strings.HasPrefix(asString(s, "key"), "5") {
														err5xx += getFloat(s, "requests")
													}
												}
												errors4xxGauge.WithLabelValues(results[i].Zone, period).Set(err4xx)
												errors5xxGauge.WithLabelValues(results[i].Zone, period).Set(err5xx)

												var rd *RangeData
												switch period {
												case "day1":
													rd = &results[i].Day1
												case "day7":
													rd = &results[i].Day7
												case "day30":
													rd = &results[i].Day30
												}
												if rd != nil {
													calcRates(rd, statusMapForCalc)
												}

												sslMap, _ := safeGetArray(sum["clientSSLMap"])
												for _, item := range sslMap {
													s, _ := safeGetMap(item)
													sslProtocolGauge.WithLabelValues(results[i].Zone, period, asString(s, "key")).Set(getFloat(s, "requests"))
												}
												browserMap, _ := safeGetArray(sum["browserMap"])
												for _, item := range browserMap {
													s, _ := safeGetMap(item)
													browserGauge.WithLabelValues(results[i].Zone, period, asString(s, "key")).Set(getFloat(s, "pageViews"))
												}
												contentTypeMap, _ := safeGetArray(sum["contentTypeMap"])
												for _, item := range contentTypeMap {
													s, _ := safeGetMap(item)
													ct := asString(s, "key")
													if ct == "" {
														ct = "unknown"
													}
													contentTypeRequestsGauge.WithLabelValues(results[i].Zone, period, ct).Set(getFloat(s, "requests"))
													contentTypeBytesGauge.WithLabelValues(results[i].Zone, period, ct).Set(getFloat(s, "bytes"))
												}
												ipClassMap, _ := safeGetArray(sum["ipClassMap"])
												for _, item := range ipClassMap {
													s, _ := safeGetMap(item)
													ipClassGauge.WithLabelValues(results[i].Zone, period, asString(s, "key")).Set(getFloat(s, "requests"))
												}
												threatPathMap, _ := safeGetArray(sum["threatPathingMap"])
												for _, item := range threatPathMap {
													s, _ := safeGetMap(item)
													threatPathGauge.WithLabelValues(results[i].Zone, period, asString(s, "key")).Set(getFloat(s, "requests"))
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		since1dTimeStr := since1d.Format("2006-01-02T15:04:05Z")
		untilDayTimeStr := untilDay.Format("2006-01-02T15:04:05Z")
		gqlHttp, _ := callGQL(buildHttpVersionQuery(zoneIDs, since1dTimeStr, untilDayTimeStr))
		if gqlHttp != nil {
			if data, ok := safeGetMap(gqlHttp["data"]); ok {
				if viewer, ok := safeGetMap(data["viewer"]); ok {
					for i := range zoneIDs {
						alias := fmt.Sprintf("z%d", i+1)
						if zoneArr, ok := safeGetArray(viewer[alias]); ok && len(zoneArr) > 0 {
							if zoneObj, ok := safeGetMap(zoneArr[0]); ok {
								if groups, ok := safeGetArray(zoneObj["httpRequestsAdaptiveGroups"]); ok {
									for _, groupItem := range groups {
										group, _ := safeGetMap(groupItem)
										dims, _ := safeGetMap(group["dimensions"])
										version := asString(dims, "clientRequestHTTPProtocol")
										requests := getFloat(group, "count")
										if version != "" {
											httpVersionGauge.WithLabelValues(results[i].Zone, "day1", version).Set(requests)
										}
									}
								}
							}
						}
					}
				}
			}
		}

		for _, r := range results {
			calcRates(&r.Day1, nil)
			calcRates(&r.Day7, nil)
			calcRates(&r.Day30, nil)

			reqGauge.WithLabelValues(r.Zone, "day1").Set(r.Day1.Requests)
			reqGauge.WithLabelValues(r.Zone, "day7").Set(r.Day7.Requests)
			reqGauge.WithLabelValues(r.Zone, "day30").Set(r.Day30.Requests)
			bandwidthGauge.WithLabelValues(r.Zone, "day1").Set(r.Day1.Bytes)
			bandwidthGauge.WithLabelValues(r.Zone, "day7").Set(r.Day7.Bytes)
			bandwidthGauge.WithLabelValues(r.Zone, "day30").Set(r.Day30.Bytes)
			pageViewsGauge.WithLabelValues(r.Zone, "day1").Set(r.Day1.PageViews)
			pageViewsGauge.WithLabelValues(r.Zone, "day7").Set(r.Day7.PageViews)
			pageViewsGauge.WithLabelValues(r.Zone, "day30").Set(r.Day30.PageViews)
			zoneUniqueVisitorsGauge.WithLabelValues(r.Zone, "day1").Set(r.Day1.UniqueVisitors)
			zoneUniqueVisitorsGauge.WithLabelValues(r.Zone, "day7").Set(r.Day7.UniqueVisitors)
			zoneUniqueVisitorsGauge.WithLabelValues(r.Zone, "day30").Set(r.Day30.UniqueVisitors)
			threatsGauge.WithLabelValues(r.Zone, "day1").Set(r.Day1.Threats)
			threatsGauge.WithLabelValues(r.Zone, "day7").Set(r.Day7.Threats)
			threatsGauge.WithLabelValues(r.Zone, "day30").Set(r.Day30.Threats)
			cacheRateGauge.WithLabelValues(r.Zone, "day1").Set(r.Day1.CacheRate)
			cacheRateGauge.WithLabelValues(r.Zone, "day7").Set(r.Day7.CacheRate)
			cacheRateGauge.WithLabelValues(r.Zone, "day30").Set(r.Day30.CacheRate)
			cachedBandwidthRateGauge.WithLabelValues(r.Zone, "day1").Set(r.Day1.CachedBandwidthRate)
			cachedBandwidthRateGauge.WithLabelValues(r.Zone, "day7").Set(r.Day7.CachedBandwidthRate)
			cachedBandwidthRateGauge.WithLabelValues(r.Zone, "day30").Set(r.Day30.CachedBandwidthRate)
			errorRateGauge.WithLabelValues(r.Zone, "day1").Set(r.Day1.ErrorRate)
			errorRateGauge.WithLabelValues(r.Zone, "day7").Set(r.Day7.ErrorRate)
			errorRateGauge.WithLabelValues(r.Zone, "day30").Set(r.Day30.ErrorRate)
		}

		gqlTop, _ := callGQL(buildTopCountriesQuery(zoneIDs, nowInLoc.Add(-24*time.Hour), nowInLoc))
		if gqlTop != nil {
			if topData, ok := safeGetMap(gqlTop["data"]); ok {
				countriesData := make(map[string]*countryData)

				if viewerTop, ok := safeGetMap(topData["viewer"]); ok {
					for i := range zoneIDs {
						alias := fmt.Sprintf("z%d", i+1)
						if zoneArr, ok := safeGetArray(viewerTop[alias]); ok && len(zoneArr) > 0 {
							if zoneMap, ok := safeGetMap(zoneArr[0]); ok {
								if arr, ok := safeGetArray(zoneMap["topCountries"]); ok {
									for _, item := range arr {
										m, _ := safeGetMap(item)
										dims, _ := safeGetMap(m["dimensions"])
										sumObj, _ := safeGetMap(m["sum"])
										countryCode := asString(dims, "clientCountryName")
										if countryCode != "" {
											if _, exists := countriesData[countryCode]; !exists {
												countryName, ok := countryCodeToName[countryCode]
												if !ok {
													countryName = countryCode // Fallback
												}
												countriesData[countryCode] = &countryData{name: countryName}
											}
											countriesData[countryCode].requests += getFloat(m, "count")
											countriesData[countryCode].bytes += getFloat(sumObj, "bytes")
										}
									}
								}
							}
						}
					}
				}

				countryReqGauge.Reset()
				countryBandwidthGauge.Reset()

				for code, data := range countriesData {
					countryReqGauge.WithLabelValues(data.name, code).Set(data.requests)
					countryBandwidthGauge.WithLabelValues(data.name, code).Set(data.bytes)
				}
			}
		}

		log.Printf("Updated metrics for %d zones", len(zoneIDs))
		exporterScrapeSuccess.Set(1)
	}

	go func() {
		for {
			update()
			log.Printf("Waiting for %d minutes before next poll...", refreshMinutes)
			time.Sleep(time.Duration(refreshMinutes) * time.Minute)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	log.Printf("Prometheus exporter listening on %s — metrics at http://localhost%s/metrics", addr, addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
