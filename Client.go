package main

import (
	"fmt"
	"crypto/md5"
	"strconv"
	"encoding/hex"
	"sort"
	"net/http"
	"io/ioutil"
	 "encoding/json"
	 "httprouter"
)

type MapData struct{
	Key int
	Value string
}

var HashMap = make(map[string]string)
var Data = make(map[int]string)
var ServerArr = []string{"3000","3001","3002"}
var SortedHashMapKeys []string

func getHash(text string)(string){
	hash := md5.Sum([]byte(text))
   	return hex.EncodeToString(hash[:])
}

func getServerForClient(k int)(string){
	    foundData := 0
		foundCache := 0
		index := 0
 		for index < len(SortedHashMapKeys){
 			if(foundData != 1){
 				if(getHash(strconv.Itoa(k)) == SortedHashMapKeys[index]){
					foundData = 1
				}	
 			}else if(foundData == 1){
 				if(stringInSlice(HashMap[SortedHashMapKeys[index]],ServerArr)){
 					foundCache = 1
 					break
 				}	
 			}
 			if(index == len(SortedHashMapKeys)-1 && foundCache == 0){
 				index = 0	
 			}else{
 				index += 1
 			}
		}
 		return HashMap[SortedHashMapKeys[index]]
}

func handlePutRequests(rw http.ResponseWriter, req *http.Request, p httprouter.Params){
	k,_ := strconv.Atoi(p.ByName("key"))
	value := p.ByName("value")
 	url := "http://localhost:"+getServerForClient(k)+"/keys/"+strconv.Itoa(k)+"/"+value
	req, err := http.NewRequest("PUT", url, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var data interface{}
	body, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(body, &data)
    var m = data.(interface{}).(float64)
 	fmt.Fprint(rw, m)
}

func handleGetRequests(rw http.ResponseWriter, req *http.Request, p httprouter.Params){
	k,_ := strconv.Atoi(p.ByName("key"))
	resp, err := http.Get("http://localhost:"+getServerForClient(k)+"/keys/"+strconv.Itoa(k))
	if(err == nil) {
        var data interface{}
		body, _ := ioutil.ReadAll(resp.Body)
		json.Unmarshal(body, &data)
	    var m = data.(map[string] interface{})        
       	mapData := new(MapData)
		mapData.Key = int(m["Key"].(float64))
		mapData.Value = m["Value"].(string)
		outgoingJSON, err := json.Marshal(mapData)
		if err != nil {
			//log.Println(error.Error())
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusCreated)
        fmt.Fprint(rw, string(outgoingJSON))
    } else {
        fmt.Println(err)
    }
}

func stringInSlice(str string, list []string) bool {
 	for _, v := range list {
 		if v == str {
 			return true
 		}
 	}
 	return false
 }

func main() {
	Data[1] = "a"
	Data[2] = "b"
	Data[3] = "c"
	Data[4] = "d"
	Data[5] = "e"
	Data[6] = "f"
	Data[7] = "g"
	Data[8] = "h"
	Data[9] = "i"
	Data[10] = "j"
	
	for _, each := range ServerArr {
    	HashMap[getHash(each)] = each
    }
	
	for k, _ := range Data {
		HashMap[getHash(strconv.Itoa(k))] = strconv.Itoa(k)
	}
	
	for k, _ :=range HashMap{
		SortedHashMapKeys = append(SortedHashMapKeys,k)
	}
	
	sort.Strings(SortedHashMapKeys)
	mux := httprouter.New()
    mux.PUT("/keys/:key/:value", handlePutRequests)
    mux.GET("/keys/:key", handleGetRequests)
    server := http.Server{
            Addr:        "0.0.0.0:8000",
            Handler: mux,
    }
    server.ListenAndServe()
}
