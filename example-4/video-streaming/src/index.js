const express = require("express");
const fs = require("fs");
const amqp = require('amqplib');

if (!process.env.RABBIT) {
    throw new Error("Please specify the name of the RabbitMQ host using environment variable RABBIT");
}

const RABBIT = process.env.RABBIT;

//
// Connect to the RabbitMQ server.
//
function connectRabbit() {

    console.log(`Connecting to RabbitMQ server at ${RABBIT}.`);

    return amqp.connect(RABBIT) // Connect to the RabbitMQ server.
        .then(connection => {
            console.log("Connected to RabbitMQ.");

            return connection.createChannel() // Create a RabbitMQ messaging channel.
                .then(messageChannel => {
                    return messageChannel.assertExchange("viewed", "fanout") // Assert that we have a "viewed" exchange.
                        .then(() => {
                            return messageChannel;
                        });
                });
        });
}

//
// Send the "viewed" to the history microservice.
//
function sendViewedMessage(messageChannel, videoPath ,receiveID) {
    console.log(`Publishing message on "viewed" exchange.`);
        
    const msg = { videoPath: videoPath, receiveID };
    const jsonMsg = JSON.stringify(msg);
    messageChannel.publish("viewed", "", Buffer.from(jsonMsg)); // Publish message to the "viewed" exchange.
}

//
// Setup event handlers.
//
function setupHandlers(app, messageChannel) {
    app.get("/video", (req, res) => { // Route for streaming video.

        let videoPath = "./videos/right_back.mp4"; // avoid unknown id if not in just let it play

        let receiveID = +req.query.id

        if(receiveID !==1 && receiveID !==2){ // Weird idea just avoid someone with wrong ID. e.g.?id=555
            videoPath = "./videos/right_back.mp4";
            fs.stat(videoPath, (err, stats) => {
                if (err) {
                    console.error("An error occurred ");
                    res.sendStatus(500);
                    return;
                }
        
                res.writeHead(200, {
                    "Content-Length": stats.size,
                    "Content-Type": "video/mp4",
                });
        
                fs.createReadStream(videoPath).pipe(res);
    
                
            }); // No need to send to history.
            
        }

        if(receiveID ===1){
            videoPath = "./videos/video2.mp4";
        }
        if(receiveID ===2){
            videoPath = "./videos/SampleVideo_1280x720_1mb.mp4";
        }

        if(receiveID ===1 || receiveID ===2){
            fs.stat(videoPath, (err, stats) => {
                if (err) {
                    console.error("An error occurred ");
                    res.sendStatus(500);
                    return;
                }
        
                res.writeHead(200, {
                    "Content-Length": stats.size,
                    "Content-Type": "video/mp4",
                });
        
                fs.createReadStream(videoPath).pipe(res);
    
                sendViewedMessage(messageChannel, videoPath ,receiveID); // Send message to "history" microservice that this video has been "viewed".
            });
        }
        // fs.stat(videoPath, (err, stats) => {
        //     if (err) {
        //         console.error("An error occurred ");
        //         res.sendStatus(500);
        //         return;
        //     }
    
        //     res.writeHead(200, {
        //         "Content-Length": stats.size,
        //         "Content-Type": "video/mp4",
        //     });
    
        //     fs.createReadStream(videoPath).pipe(res);

        //     sendViewedMessage(messageChannel, videoPath ,receiveID); // Send message to "history" microservice that this video has been "viewed".
        // });
    });
}

//
// Start the HTTP server.
//
function startHttpServer(messageChannel) {
    return new Promise(resolve => { // Wrap in a promise so we can be notified when the server has started.
        const app = express();
        setupHandlers(app, messageChannel);

        const port = process.env.PORT && parseInt(process.env.PORT) || 3000;
        app.listen(port, () => {
            resolve(); // HTTP server is listening, resolve the promise.
        });
    });
}

//
// Application entry point.
//
function main() {
    return connectRabbit()                          // Connect to RabbitMQ...
        .then(messageChannel => {                   // then...
            return startHttpServer(messageChannel); // start the HTTP server.
        });
}

main()
    .then(() => console.log("Microservice online."))
    .catch(err => {
        console.error("Microservice failed to start.");
        console.error(err && err.stack || err);
    });