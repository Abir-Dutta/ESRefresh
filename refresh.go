package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

type Configuration struct {
	EsConfiguration  ESConfiguration  `json:"esConfiguration"`
	SqlConfiguration SQLConfiguration `json:"sqlConfiguration"`
}

type ESConfiguration struct {
	SourceURL        string `json:"sourceURL"`
	DestinationURL   string `json:"destinationURL"`
	SourceIndex      string `json:"sourceIndex"`
	DestinationIndex string `json:"destinationIndex"`
	Username         string `json:"username"`
	Password         string `json:"password"`
	EntityType       string `json:"entityType"`
	BatchSize        int    `json:"batchSize"`
}

type SQLConfiguration struct {
	DbServer                string            `json:"dbServer"`
	DbName                  string            `json:"dbName"`
	Username                string            `json:"username"`
	Password                string            `json:"password"`
	EntityType              string            `json:"entityType"`
	MaximumOpenDBConnection int               `json:"maximumOpenDBConnection"`
	BatchSize               int               `json:"batchSize"`
	FilterSQL               string            `json:"filterSQL"`
	StoredProc              map[string]string `json:"storedProc"`
}

const (
	POST   = "POST"
	GET    = "GET"
	DELETE = "DELETE"
)

var configuration Configuration
var StoredProcedure []string
var BulkResponse bytes.Buffer

var wg sync.WaitGroup
var timeStarted time.Time

var FilterCount = 0
var channelCounter = 1
var BulkResponseChannel chan string
var CountBatchChannel chan int
var ScrollIdChannel string

func main() {
	timeStarted = time.Now()

	raw, err := ioutil.ReadFile("./config.json")
	if err != nil {
		log.Fatalln("Problem with configuration file! ... Reason: ", err)
	}
	err = json.Unmarshal(raw, &configuration)
	if err != nil {
		log.Fatalln("Problem with configuration file! ... Reason: ", err)
	} else {
		//getSQLDatabase()
		getESDatabase()
	}
}
func getEsResponse(destinationESURL, BulkResquest, method string) (string, error) {

	client := &http.Client{
		Timeout: 15 * time.Minute,
	}
	req, err := http.NewRequest(method, destinationESURL, bytes.NewBufferString(BulkResquest))
	req.SetBasicAuth(configuration.EsConfiguration.Username, configuration.EsConfiguration.Password)
	req.Header.Set("Content-Type", "application/json;")
	//fmt.Println(req)
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
	}
	//fmt.Println(resp)
	respBody, respErr := ioutil.ReadAll(resp.Body)
	//if respErr != nil {
	//fmt.Println("REquest", BulkResquest)
	//fmt.Println("REsponse", resp)
	//}
	responseString := string(respBody)
	//fmt.Println(responseString)
	return responseString, respErr
}
func createBulkRequest(bulkResponseToAdd, entityID string) {
	BulkResponse.WriteString("{\"update\" : {" + "\"_index\" : \"" + configuration.EsConfiguration.DestinationIndex + "\", \"_type\" : \"" + configuration.SqlConfiguration.EntityType + "\", \"_id\" : \"" + entityID + "\"}} \n{ \"doc\" : " + bulkResponseToAdd + " ,\"doc_as_upsert\":true} \n")
}
func displayProgress(runningBatchNo int, batchRequest string) {

	bulkRespJSON, bulkRespErr := getEsResponse(configuration.EsConfiguration.DestinationURL+"_bulk", batchRequest, POST)
	if bulkRespErr != nil {
		log.Println(bulkRespErr)
		fmt.Println("Processing Batch of ", configuration.SqlConfiguration.EntityType, " # ", runningBatchNo, " - Failed ... Reason: ", bulkRespJSON)
	} else {
		if strings.Contains(bulkRespJSON, "\"status\":20") {
			fmt.Println("Processing Batch of ", configuration.SqlConfiguration.EntityType, " # ", runningBatchNo, " - Success ... ")
		} else {
			fmt.Println("Processing Batch of ", configuration.SqlConfiguration.EntityType, " # ", runningBatchNo, " - Failed ... Reason: ", bulkRespJSON)
		}
	}
}
func getESDatabase() {
	bulkRespJSON, bulkRespErr := getEsResponse(
		configuration.EsConfiguration.SourceURL+
			configuration.EsConfiguration.SourceIndex+
			"/"+configuration.EsConfiguration.EntityType+
			"/_search",
		"{\"size\": 0}", POST)
	if bulkRespErr != nil {
		log.Fatalln("Error in ES Request ... Reason: ", bulkRespErr)
	} else {

		var esJSONResponse map[string]interface{}
		byt := []byte(bulkRespJSON)
		responseUnMarshalErr := json.Unmarshal(byt, &esJSONResponse)
		if responseUnMarshalErr != nil {
			log.Fatalln("Unable to Parse ES Response ... Reason: ", responseUnMarshalErr)
		} else {
			byt, _ = json.Marshal(esJSONResponse["hits"])
			json.Unmarshal(byt, &esJSONResponse)
			FilterCount = int(esJSONResponse["total"].(float64))
			fmt.Println("Updating", FilterCount, configuration.SqlConfiguration.EntityType, "record(s)")

		}
		for i := 0; i < FilterCount/configuration.EsConfiguration.BatchSize; i++ {

			prepareBulkRequestObjectES(i)
			displayProgress(i, BulkResponse.String())
			BulkResponse.Reset()
		}

		fmt.Printf("Time esapsed %0.3f Sec\n", time.Since(timeStarted).Seconds())
	}
}
func prepareBulkRequestObjectES(runningBatchNo int) {

	var bulkRespJSON string
	var bulkRespErr error
	if runningBatchNo == 0 {
		bulkRespJSON, bulkRespErr = getEsResponse(
			configuration.EsConfiguration.SourceURL+
				configuration.EsConfiguration.SourceIndex+
				"/"+configuration.EsConfiguration.EntityType+
				"/_search?scroll=10m",
			"{\"size\":"+strconv.Itoa(configuration.EsConfiguration.BatchSize)+"}", POST)
	} else {
		bulkRespJSON, bulkRespErr = getEsResponse(
			configuration.EsConfiguration.SourceURL+
				"_search/scroll",
			"{\"scroll\" : \"10m\",\"scroll_id\":"+ScrollIdChannel+"}", POST)
	}
	if bulkRespErr != nil {
		log.Println("Error in ES Request ... Reason: ", bulkRespErr)
	} else {
		var esJSONResponse map[string]interface{}
		byt := []byte(bulkRespJSON)
		responseUnMarshalErr := json.Unmarshal(byt, &esJSONResponse)
		if responseUnMarshalErr != nil {
			log.Println("Unable to Parse ES Response ... Reason: ", responseUnMarshalErr)
		} else {
			var esHitHitResponse []map[string]interface{}
			byt, _ = json.Marshal(esJSONResponse["_scroll_id"])
			ScrollIdChannel = string(byt)

			byt, _ = json.Marshal(esJSONResponse["hits"])

			responseUnMarshalErr = json.Unmarshal(byt, &esJSONResponse)
			if responseUnMarshalErr != nil {
				log.Println("Unable to Parse ES Response ... Reason: ", responseUnMarshalErr)
			} else {
				byt, _ = json.Marshal(esJSONResponse["hits"])
				responseUnMarshalErr = json.Unmarshal(byt, &esHitHitResponse)
				if responseUnMarshalErr != nil {
					log.Println("Unable to Parse ES Response ... Reason: ", responseUnMarshalErr)
				} else {
					for _, hitObject := range esHitHitResponse {
						byt, responseUnMarshalErr = json.Marshal(hitObject["_source"])
						if responseUnMarshalErr != nil {
							log.Println("Unable to Parse ES Response ... Reason: ", responseUnMarshalErr)
						} else {
							documentID, responseUnMarshalErr := json.Marshal(hitObject["_id"])
							if responseUnMarshalErr != nil {
								log.Println("Unable to Parse ES Response ... Reason: ", responseUnMarshalErr)
							} else {
								createBulkRequest(string(byt), strings.Trim(string(documentID), "\""))
							}
						}
					}
				}
			}

		}
	}
}
