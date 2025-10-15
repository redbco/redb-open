package engine

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// transformUppercase converts input to uppercase
func transformUppercase(input string) string {
	return strings.ToUpper(input)
}

// transformLowercase converts input to lowercase
func transformLowercase(input string) string {
	return strings.ToLower(input)
}

// transformReverse reverses the input string
func transformReverse(input string) string {
	runes := []rune(input)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

// transformBase64Encode encodes input to base64
func transformBase64Encode(input string) string {
	return base64.StdEncoding.EncodeToString([]byte(input))
}

// transformBase64Decode decodes base64 input
func transformBase64Decode(input string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(input)
	if err != nil {
		return "", fmt.Errorf("invalid base64 input: %v", err)
	}
	return string(decoded), nil
}

// transformJSONFormat formats and validates JSON input
func transformJSONFormat(input string) (string, error) {
	var jsonData interface{}
	if err := json.Unmarshal([]byte(input), &jsonData); err != nil {
		return "", fmt.Errorf("invalid JSON input: %v", err)
	}
	formatted, err := json.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to format JSON: %v", err)
	}
	return string(formatted), nil
}

// transformXMLFormat formats XML input (basic formatting)
func transformXMLFormat(input string) (string, error) {
	// Basic XML validation by attempting to unmarshal
	var v interface{}
	if err := xml.Unmarshal([]byte(input), &v); err != nil {
		return "", fmt.Errorf("invalid XML input: %v", err)
	}
	// For now, just return the input as-is since xml.MarshalIndent is more complex
	return input, nil
}

// transformCSVToJSON converts CSV to JSON
func transformCSVToJSON(input string) (string, error) {
	reader := csv.NewReader(strings.NewReader(input))
	records, err := reader.ReadAll()
	if err != nil {
		return "", fmt.Errorf("invalid CSV input: %v", err)
	}

	if len(records) == 0 {
		return "[]", nil
	}

	// Use first row as headers
	headers := records[0]
	var jsonData []map[string]string

	for i := 1; i < len(records); i++ {
		row := make(map[string]string)
		for j, value := range records[i] {
			if j < len(headers) {
				row[headers[j]] = value
			}
		}
		jsonData = append(jsonData, row)
	}

	result, err := json.Marshal(jsonData)
	if err != nil {
		return "", fmt.Errorf("failed to convert to JSON: %v", err)
	}

	return string(result), nil
}

// transformJSONToCSV converts JSON to CSV
func transformJSONToCSV(input string) (string, error) {
	var jsonData []map[string]interface{}
	if err := json.Unmarshal([]byte(input), &jsonData); err != nil {
		return "", fmt.Errorf("invalid JSON input: %v", err)
	}

	if len(jsonData) == 0 {
		return "", nil
	}

	// Extract headers from first object
	var headers []string
	for key := range jsonData[0] {
		headers = append(headers, key)
	}

	var csvData [][]string
	csvData = append(csvData, headers)

	// Convert each object to CSV row
	for _, obj := range jsonData {
		var row []string
		for _, header := range headers {
			value := ""
			if val, exists := obj[header]; exists && val != nil {
				value = fmt.Sprintf("%v", val)
			}
			row = append(row, value)
		}
		csvData = append(csvData, row)
	}

	// Build CSV string
	var output strings.Builder
	writer := csv.NewWriter(&output)
	defer writer.Flush()

	for _, row := range csvData {
		if err := writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write CSV: %v", err)
		}
	}

	return output.String(), nil
}

// transformHashSHA256 generates SHA256 hash
func transformHashSHA256(input string) string {
	hash := sha256.Sum256([]byte(input))
	return fmt.Sprintf("%x", hash)
}

// transformHashMD5 generates MD5 hash
func transformHashMD5(input string) string {
	hash := md5.Sum([]byte(input))
	return fmt.Sprintf("%x", hash)
}

// transformURLEncode URL encodes the input
func transformURLEncode(input string) string {
	return url.QueryEscape(input)
}

// transformURLDecode URL decodes the input
func transformURLDecode(input string) (string, error) {
	decoded, err := url.QueryUnescape(input)
	if err != nil {
		return "", fmt.Errorf("invalid URL encoded input: %v", err)
	}
	return decoded, nil
}

// transformTimestampToISO converts Unix timestamp to ISO 8601
func transformTimestampToISO(input string) (string, error) {
	timestamp, err := strconv.ParseInt(input, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid timestamp: %v", err)
	}

	t := time.Unix(timestamp, 0)
	return t.Format(time.RFC3339), nil
}

// transformISOToTimestamp converts ISO 8601 to Unix timestamp
func transformISOToTimestamp(input string) (string, error) {
	t, err := time.Parse(time.RFC3339, input)
	if err != nil {
		return "", fmt.Errorf("invalid ISO 8601 format: %v", err)
	}

	return strconv.FormatInt(t.Unix(), 10), nil
}

// transformDirectMapping returns the input as-is (passthrough/default transformation)
func transformDirectMapping(input string) string {
	return input
}

// transformUUIDGenerator generates a random UUID (generator type - no source required)
func transformUUIDGenerator() string {
	return uuid.New().String()
}

// transformNullExport exports data without returning anything (null_returning type)
// This function would typically send data to an external interface
// For now, it's a placeholder that just returns empty string
func transformNullExport(input string) string {
	// TODO: Implement actual export logic (e.g., send to external API, log, etc.)
	// For now, we just acknowledge receipt and return empty
	return ""
}
