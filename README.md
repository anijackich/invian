# Road monitoring system 
Backend part of the road monitoring service for the Invian hackathon

## ⬇️ Downloading
Clone the repository to your machine:
```shell
git clone https://github.com/anijackich/invian/
cd invian
```

## ⚙️ Setup the environment
Rename `.env.example` to `.env` and specify the Kafka and Websockets configuration in this file.  
To correct the coordinate offset, create a file `offset_conf.json` with the following content:
```json
{
  "x_offset": 0.0,
  "y_offset": 0.0,
  "rotation_angle": 6.0,
  "rotation_origin": [55.797654, 49.2436269]
}
```
`x_offset` and `y_offset` — offset along the axes  
`rotation_angle` and `rotation_origin` — rotational displacement by the angle relative to a point

## 🚀 Run
Run an application using Docker:
```shell
docker compose up -d --build
```