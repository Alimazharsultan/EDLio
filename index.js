const server = require('http').createServer();
const mongoose = require('mongoose')
const EntryModel = require('./models/location3data')
const cors = require('cors')
app.use(cors())
//For Mqtt
const mqtt = require("mqtt");
require('dotenv').config();

var valueRecieved = false;
var ACValue = 0;
var resetValue=0;


const io= require('socket.io')(server,{
    transports: ['websocket', 'polling']
  });

  //Variables
var tempS = '';
var humidityS = '';
var pressureS = '';
var lumS = '';
// var timeFormated = '';

//Connecting to mqtt
var options={
      port: "1883",
      protocol: "mqtt",
      clientId: "NodeJSclient",
      hostname: "broker.hivemq.com",
    };

var client = mqtt.connect(options);
client.subscribe("AIEMSL1/EDL_0001");
client.subscribe("AIEMSL1/EDL_0003");
// console.log("connected  "+client.connected);
io.on("AC", (message)=>{
    console.log('recieved:')
})
client.on('message', function(topic, msg){
  console.log(topic+" Message Recieved -> "+msg.toString());
  if(topic.toString()==="AIEMSL1/EDL_0001"){
    const obj = JSON.parse(msg.toString());
    tempS = obj.Tem;
    humidityS = obj.Hum;
    pressureS = obj.Pres;
    lumS = obj.Lu;

    valueRecieved = true;
    io.emit('cpu',{ temp: tempS, humidity: humidityS, pressure: pressureS, lum: lumS });
  
  }
  if(topic.toString()==="AIEMSL1/EDL_0003"){
    const obj = JSON.parse(msg.toString());
    io.emit('EDL_0002',{ temp: obj.Tem, humidity: obj.Hum, pressure: obj.Pres, lum: obj.Lu });
    
    const event = new EntryModel({
      readingtime: new Date().toISOString(),
      temperature: obj.Tem,
      humidity: obj.Hum,
      pressure: obj.Pres,
      altitude: obj.Lu,
      temperature_status: "Coming Soon",
      humidity_status: "Coming Soon",
      pressure_status: "Coming Soon",
    });
    return event.save().then((r)=>{
      console.log('EDL_0003 saved to database');
    }    
    ).catch(err=>{
      console.log('Error saving to database');
            }); 
    }
  
  //Send Data to ac
  if(ACValue!=0){
    if(client.connected){
      client.publish('AIEMSL1/EDL_0001I', JSON.stringify({ Delay: ACValue.toString(),Reset:0 }),opts=options);
      console.log('Delay Value sent again');
      ACValue = 0;
        
      
    }else{
      console.log('Ac Value not sent');
    }
    ACValue=0
  }

  //Send reset value
  if(resetValue!=0){
    if(client.connected){
      client.publish('AIEMSL1/EDL_0001I', JSON.stringify({ Reset:1 }),opts=options);
      console.log('Wifi Reset sent again');
      resetValue = 0;
      
    }else{
      
    }
    resetValue=0
  }
  
  // emit to sockets.io
  
  if(valueRecieved){
    
    const event = new EntryModel({
      readingtime: new Date().toISOString(),
      temperature: tempS,
      humidity: humidityS,
      pressure: pressureS,
      altitude: lumS,
      temperature_status: "Coming Soon",
      humidity_status: "Coming Soon",
      pressure_status: "Coming Soon",
    });
    return event.save().then((r)=>{
      console.log('saved to database');
      
    }    
    ).catch(err=>{
      console.log('Error saving to database');
            });  
    // axios.post("http://localhost:3001/createEntry", {
    //           readingtime: date_ob,
    //           temperature: tempS,
    //           humidity: humidityS,
    //           pressure: pressureS,
    //           altitude: lumS,
    //           temperature_status: "Coming Soon",
    //           humidity_status: "Coming Soon",
    //           pressure_status: "Coming Soon",
    //         }).then((response) => {
    //           console.log('Data Sent')
    //           valueRecieved=0;
    //       }).catch(err=>{
    //         throw err;
    //       });   
  }
})

setInterval(()=>{
  if(!valueRecieved){
    io.emit('cpu',{value: 'NaN'});
   
    console.log('values reset')
  }
  valueRecieved=false;
 
  }, 950000);

  
  
io.on("connection", (socket) => {
//   console.log('MQTT connection established')
  if(tempS!=0 && humidityS!=0){
    io.emit('cpu',{ temp: tempS, humidity: humidityS, pressure: pressureS, lum: lumS });
  }
  
  socket.on("interval",(m)=>{
    if(client.connected){
        client.publish('AIEMSL1/EDL_0001I', JSON.stringify({ Delay: m.value.toString(),Reset:0 }),opts=options);
        console.log('Delay Value sent');
        ACValue = 0;
        
      }else{
        ACValue = m.value;
      }
})
socket.on("wifiReset",(args)=>{
  
  if(client.connected){
      client.publish('AIEMSL1/EDL_0001I', JSON.stringify({ Reset:1 }),opts=options);
      console.log('Wifi Reset sent');
      resetValue = 0;
      
    }else{
      resetValue=1;
      console.log('Wifi Reset Not Sent');
    }
})
});

mongoose
  .connect(
    `mongodb+srv://ali:great@cluster0.p3ddg.mongodb.net/merntutorial?retryWrites=true&w=majority`
  )
  .then(() => {
    console.log('Database Server Running')
       server.listen(process.env.PORT|| 4000, () => {
      console.log("Sockets Server Running");
    });
    
  })
  .catch(err => {
    console.log(err);
  });
