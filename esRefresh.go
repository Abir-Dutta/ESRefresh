package main

import (
	"bytes"
	"database/sql"
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

var configuration Configuration
var StoredProcedure []string
var BulkResponse bytes.Buffer

var wg sync.WaitGroup
var timeStarted time.Time

var FilterCount = 0
var channelCounter = 1
var BulkResponseChannel chan string
var CountBatchChannel chan int
var FailedBatchChanel chan string

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
		getSQLDatabase()
	}
}
func getEsResponse(destinationESURL, BulkResquest string) (string, error) {

	client := &http.Client{
		Timeout: 15 * time.Minute,
	}
	req, err := http.NewRequest("POST", destinationESURL, bytes.NewBufferString(BulkResquest))
	req.SetBasicAuth(configuration.EsConfiguration.Username, configuration.EsConfiguration.Password)
	req.Header.Set("Content-Type", "application/json;")
	//fmt.Println(req)
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
	}
	//fmt.Println(resp)
	respBody, respErr := ioutil.ReadAll(resp.Body)
	if respErr != nil {
		fmt.Println("REquest", BulkResquest)
		fmt.Println("REsponse", resp)
	}
	responseString := string(respBody)
	//fmt.Println(responseString)
	return responseString, respErr
}
func createBulkRequest(bulkResponseToAdd, entityID string) {
	BulkResponse.WriteString("{\"update\" : {" + "\"_index\" : \"" + configuration.EsConfiguration.DestinationIndex + "\", \"_type\" : \"" + configuration.SqlConfiguration.EntityType + "\", \"_id\" : \"" + entityID + "\"}} \n{ \"doc\" : " + bulkResponseToAdd + " ,\"doc_as_upsert\":true} \n")
}
func displayProgress(runningBatchNo int, batchRequest string) {
	wg.Add(1)
	defer wg.Done()
	bulkRespJSON, bulkRespErr := getEsResponse(configuration.EsConfiguration.DestinationURL+"_bulk", batchRequest)
	if bulkRespErr != nil {
		log.Println(bulkRespErr)
		fmt.Println("Processing Batch of ", configuration.SqlConfiguration.EntityType, " # ", runningBatchNo, " - Failed ... Reason: ", bulkRespJSON)
	} else {
		if strings.Contains(bulkRespJSON, "\"status\":20") {
			fmt.Println("Processing Batch of ", configuration.SqlConfiguration.EntityType, " # ", runningBatchNo, " - Success ... ")
		} else {
			fmt.Println("Processing Batch of ", configuration.SqlConfiguration.EntityType, " # ", runningBatchNo, " - Failed ... Reason: ", bulkRespJSON)
			FailedBatchChanel <- bulkRespJSON
		}
	}
}
func prepareBulkRequestObjectSP(db *sql.DB, rowidObject string) {
	entitysprow, entitysperr := db.Query("set nocount on; exec "+StoredProcedure[0]+" ?", rowidObject)
	var dbEntityJSON, dbOperation sql.NullString
	var entityJSON string
	if entitysperr != nil {
		//panic(entitysperr)
		log.Println("Entity Stored Proc Error: Rowid_object: ", rowidObject, " -- ", entitysperr)
		BulkResponse.Reset()
		//os.Exit(0)
	} else {
		for entitysprow.Next() {
			entitysperr = entitysprow.Scan(&dbEntityJSON, &dbOperation)
			if entitysperr != nil {
				//panic(entitysperr)

				log.Println("Entity Stored Proc Error: Rowid_object: ", rowidObject, " -- ", entitysperr)
				BulkResponse.Reset()
				//os.Exit(0)
			}
		}
		//fmt.Println(dbEntityJSON.)
		entityJSON = dbEntityJSON.String

		var JSONSchema []map[string]interface{}
		byt := []byte(entityJSON)
		//if entityJSON != "" {

		if err := json.Unmarshal(byt, &JSONSchema); err != nil {
			//panic(err)
			log.Println("Entity Stored Proc JSON Error: Rowid_object: ", rowidObject, " -- ", err)
			BulkResponse.Reset()
		} else {
			//fmt.Println(entityJSON)
			if len(StoredProcedure) > 1 {
				propertysprow := db.QueryRow("set nocount on; exec "+StoredProcedure[1]+" ?", rowidObject)
				var dbPropertyJSON sql.NullString
				var propertyJSON string
				propertysperr := propertysprow.Scan(&dbPropertyJSON)

				if propertysperr != nil {
					//panic(propertysperr)
					log.Println("Get Property Stored Proc Error: Rowid_object: ", rowidObject, " -- ", propertysperr)
					BulkResponse.Reset()
				} else {
					//for propertysprow.Next() {
					if propertysperr != nil {
						//panic(propertysperr)
						log.Println("Get Property Stored Proc Error: Rowid_object: ", rowidObject, " -- ", propertysperr)
						BulkResponse.Reset()
						//os.Exit(0)
					}
					//}
					propertyJSON = dbPropertyJSON.String
					//fmt.Println(propertyJSON)
					//fcombinedJSON, _ := json.Unmarshal(propertyJSON)
					var PropJSONSchema map[string]interface{}
					byt = []byte(propertyJSON)
					json.Unmarshal(byt, &PropJSONSchema)
					//fmt.Println(err, PropJSONSchema)
					JSONSchema[0]["property"] = PropJSONSchema
					combinedJSON, _ := json.Marshal(JSONSchema[0])

					entityJSON = string(combinedJSON)
					createBulkRequest(entityJSON, rowidObject)
				}
			} else {
				//Only For property
				for _, item := range JSONSchema {
					tempJSON, _ := json.Marshal(item)
					createBulkRequest(string(tempJSON), strconv.FormatFloat(item["PROP_CLSFN_ID"].(float64), 'f', 0, 64))
					//fmt.Println(string(a))
				}
			}
		}
		if BulkResponse.Len() != 0 {
			BulkResponseChannel <- BulkResponse.String()
			BulkResponse.Reset()
		}
		CountBatchChannel <- channelCounter
		channelCounter++
	}

	//defer wg.Done()
}
func getSQLDatabase() {
	var connectionString = "server=" + configuration.SqlConfiguration.DbServer +
		";user id=" + configuration.SqlConfiguration.Username +
		";password=" + configuration.SqlConfiguration.Password +
		";database=" + configuration.SqlConfiguration.DbName +
		";app name=ESRefresh" +
		";connection timeout=0"

	db, _ := sql.Open("mssql", connectionString)
	//fmt.Println("OPEN: ", db.Ping())
	db.SetMaxOpenConns(configuration.SqlConfiguration.MaximumOpenDBConnection)
	//db.SetMaxIdleConns(0)
	rows, err := db.Query(configuration.SqlConfiguration.FilterSQL)

	FilterCount = 0
	BulkResponseChannel = make(chan string, configuration.SqlConfiguration.BatchSize)
	CountBatchChannel = make(chan int, 30)
	StoredProcedure = strings.Split(configuration.SqlConfiguration.StoredProc[configuration.SqlConfiguration.EntityType], ",")

	if err != nil {
		log.Println(err)
	} else {

		for rows.Next() {
			var rowidObject string
			err := rows.Scan(&rowidObject)
			if err != nil {
				log.Println(err)
			}
			FilterCount++

			//wg.Add(1)
			go prepareBulkRequestObjectSP(db, rowidObject)
		}
	}
	//For All Batch
	fmt.Println("Updating", FilterCount, configuration.SqlConfiguration.EntityType, "record(s)")
	var batchRequest string = ""
	runningBatchNo := 1
	for {
		bulkReqeustChanelData := <-BulkResponseChannel
		if bulkReqeustChanelData != "" {
			batchRequest += bulkReqeustChanelData
			if runningBatchNo%configuration.SqlConfiguration.BatchSize == 0 {

				go displayProgress(runningBatchNo/configuration.SqlConfiguration.BatchSize, batchRequest)
				batchRequest = ""
			}

			if runningBatchNo >= FilterCount {
				break
			}
			runningBatchNo++
		}
	}
	if batchRequest != "" {
		go displayProgress(runningBatchNo/configuration.SqlConfiguration.BatchSize+1, batchRequest)
		batchRequest = ""
	}
	wg.Wait()
	fmt.Printf("Time esapsed %0.3f Sec\n", time.Since(timeStarted).Seconds())
	defer db.Close()

}
