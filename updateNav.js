// Run this script everyday.
const fs = require('fs');
const csv = require('csv-parser');
const request = require('request');
const AWS = require('aws-sdk');
const documentClient = new AWS.DynamoDB.DocumentClient();

exports.handler = function(event, context, callback) {
  const totalLines = 0;
  let results = [];
  let recordChunks = [];

  request.get(process.env.URL_NAV_DATA).on('response', function(response) {
    response
      .pipe(csv({ separator: ';' }))
      .on('data', data => results.push(data))
      .on('end', () => {
        results = results.filter(function(result) {
          return (
            !isNaN(result['Scheme Code']) && parseInt(result['Scheme Code']) > 0
          );
        });
        // Create a chunk 20 items as there is a upper limit of 25 items
        // and 16MB of data.
        recordChunks = chunk(results, 20);

        const TABLE_NAME = process.env.TABLE_NAV_DATA;

        recordChunks.map(resultsData => {
          let mfData = resultsData.map(item => {
            return {
              PutRequest: {
                Item: {
                  code: item['Scheme Code'],
                  schemeName: item['Scheme Name'],
                  nav: item['Net Asset Value'],
                  date: item['Date']
                }
              }
            };
          });

          let params = {
            RequestItems: {
              [TABLE_NAME]: mfData
            }
          };

          documentClient.batchWrite(params, function(err, data) {
            callback(err, data);
          });
        });
      });
  });
};

function chunk(array, size) {
  const chunkedArr = [];
  let index = 0;
  while (index < array.length) {
    chunkedArr.push(array.slice(index, size + index));
    index += size;
  }
  return chunkedArr;
}
