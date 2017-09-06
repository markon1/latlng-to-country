var request = require('request'),
    json2csv = require('json2csv'),
    winston = require('winston'),
    fs = require('fs'),
    csvtojson = require('csvtojson'),
    gc = require('country-reverse-geocoding').country_reverse_geocoding(),
    dotenv = require('dotenv').config();


// Logger init
var logger = new(winston.Logger)({
    transports: [
        new(winston.transports.Console)({ json: false, timestamp: true }),
        new winston.transports.File({ filename: __dirname + '/debug.log', json: false })
    ],
    exceptionHandlers: [
        new(winston.transports.Console)({ json: false, timestamp: true }),
        new winston.transports.File({ filename: __dirname + '/exceptions.log', json: false })
    ],
    exitOnError: true
});

// Input filename
var inputCSV = "";
// Loaded posts
var posts = [];
// Processed posts
var done = [];
// Done counter;
var doneCounter = 0;
// Counter for countries not found using 'country-reverse-geocoding', used for avoiding being banned by API
var missCounter = 0;
// Milliseconds between two API calls
var pause = 150;
// to use API key or not
var useKey = false;
// boolean which checks if query_limit is already hit
var query_limit_hit = false;

// Read from command line
if (process.argv.length >= 3) {
    inputCSV = process.argv[2];
    if (!!process.argv[3] && process.argv[3] == 'y') {
        useKey = true;
    }
    readCSV(inputCSV);
} else {
    logger.error("Error loading command line arguments");
}

// Function for reading CSV from input file and finding country
function readCSV(csv) {
    logger.info("Reading input CSV...");

    csvtojson()
        .fromFile(csv)
        .on('json', (jsonObj) => {
            posts.push(jsonObj);
        })
        .on('done', (error) => {
            logger.info('Fetching countries...');
            for (i = 0; i < posts.length; i++) {
                done[i] = posts[i];
                // if it hasn't country already
                if (!done[i].country) {
                    // if it can be found locally
                    if (!!gc.get_country(Number(posts[i].location_latitude), Number(posts[i].location_longitude))) {
                        done[i].country = gc.get_country(Number(posts[i].location_latitude), Number(posts[i].location_longitude)).name;
                        doneCounter++;
                        if (doneCounter == posts.length) {
                            makeCSV();
                        }
                        // if we must use Google's API
                    } else {

                        function fnc(j) {
                            if (useKey) {
                                findCountries(j, "https://maps.googleapis.com/maps/api/geocode/json", { latlng: posts[j].location_latitude + "," + posts[j].location_longitude, key: process.env.GOOGLE_API_KEY });
                            } else {
                                setTimeout(function() { findCountries(j, "https://maps.googleapis.com/maps/api/geocode/json", { latlng: posts[j].location_latitude + "," + posts[j].location_longitude }); }, missCounter * pause);
                            }
                        }

                        fnc(i);

                        missCounter++;
                    }
                } else {
                    doneCounter++;
                }
            }
        });
}

// Function for finding countries
function findCountries(ind, uri, qs) {
    request({
        uri: uri,
        qs: qs,
        type: 'json',
        timeout: 5000
    }, function(error, response, body) {
        if (!query_limit_hit) {
            if (!!body) {
                body = JSON.parse(body);
                if (!!body.status && body.status === 'OK') {
                    for (i = 0; i < body.results[0].address_components.length; i++) {
                        var address_component = body.results[0].address_components[i];
                        for (j = 0; j < address_component.types.length; j++) {
                            if (address_component.types[j] == 'country') {
                                done[ind].country = address_component.long_name;
                                break;
                            }
                        }
                    }

                    doneCounter++;
                    // If all posts are done, make CSV
                    if (doneCounter == posts.length) {
                        makeCSV();
                    }

                    // In this case there is connection error, so we log it and try again
                } else if (!!body && !!body.status && body.status === "ZERO_RESULTS") {
                    logger.warn('No result for ' + qs.latlng);
                    doneCounter++;
                    // If all posts are done, make CSV
                    if (doneCounter == posts.length) {
                        makeCSV();
                    }

                    // In this case there was an error with access token validity, so app logs error and stops here
                } else if (!!body && !!body.status && body.status === "OVER_QUERY_LIMIT") {
                    query_limit_hit = true;
                    logger.warn('Error: OVER_QUERY_LIMIT, exporting to CSV... ');
                    makeCSV();
                    logger.error('Error: OVER_QUERY_LIMIT');
                    // In this case there was an error with access token validity, so app logs error and stops here
                } else {
                    logger.warn('Error finding country for ' + qs.latlng);
                    logger.info('Trying again to find country for: ' + qs.latlng);
                    findCountries(ind, uri, qs);
                }
            } else {
                logger.warn('Error finding country for ' + qs.latlng);
                logger.info('Trying again to find country for: ' + qs.latlng);
                findCountries(ind, uri, qs);
            }
        }
    });
}

// Function that makes use of json2csv package to save fetched posts to CSV file
function makeCSV() {
    logger.info('Creating CSV file...');

    var csv = json2csv({ data: posts, newLine: "\r\n" });
    var outputFilename = inputCSV + '-withCountries-' + new Date().getTime() + '.csv';

    fs.writeFile(outputFilename, csv, function(err) {
        if (err) {
            logger.error('Error encountered while exporting posts to ' + outputFilename + ' file. More details: ' + err);
        } else {
            logger.info('Successfully exported posts to ' + outputFilename + ' file.');
        }
    });
}
