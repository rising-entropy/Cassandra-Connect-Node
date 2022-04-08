const express = require('express')
const app = express()
const axios = require('axios')
const { v4 } = require('uuid')
const cassandra = require('cassandra-driver');

const client = new cassandra.Client({
    contactPoints: ["10.4.2.96","10.4.2.112","10.4.2.105"],
    localDataCenter: 'datacenter1',
    keyspace: 'weather',
    credentials: { username: 'cassandra', password: 'cassandra' }
});


app.get('/', function (req, res) {
    const query = `
    CREATE TABLE emp(
        emp_id int PRIMARY KEY,
        emp_name text,
        emp_city text,
        emp_sal varint,
        emp_phone varint
        );
    `;
    client.execute(query, [ ])
      .then(result => console.log(result));
});

app.post('/add-report', async(req, res)=>{

    let lat = "51.5072"
    let long = "0.1276"

    let body = {}

    axios.get(`https://api.openweathermap.org/data/2.5/onecall?lat=${lat}&lon=${long}&exclude=minutely,hourly,daily&appid=4fc57c093b1962e1f5db11a8b893119c`)
    .then((response)=>{
        let data = response.data;
        body["lat"] = data["lat"]
        body["long"] = data["lon"]
        body["dt"] = data["current"]["dt"]
        body["temp"] = data["current"]["temp"]
        body["windspeed"] = data["current"]["wind_speed"]

        const query = `
        insert into weather.weatherInfo(
            id, dt, lat, long, temp, windspeed
        ) values(
            '${String(v4())}', '${String(body['dt'])}', '${String(lat)}', '${String(long)}', '${String(body['temp'])}', '${String(body['windspeed'])}'
        );
        `;

        client.execute(query, [ ])
        .then(result => console.log(result));

        return res.status(201).json({
            message: "Added Successfully!"
        })

    })
    .catch((err)=>{
        console.log(err)
        return res.status(500).json({
            message: "Some Error Occurred!"
        })
    });
})

app.get('/all-reports', async(req, res)=>{

    let theRequired = []
    const query = `
        select * from weather.weatherinfo;
        `;
    client.execute(query, [ ])
    .then(result => {
        return res.json(result.rows)
    });
})

app.listen(3000)