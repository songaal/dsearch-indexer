# FastcatX 색인기

신규 패스트캣은 ES에 데이터를 저장하므로, 색인모듈이 필요하지 않다. 

하지만 데이터소스에서 ES로 데이터를 연결해주는 파이프라인의 역할은 여전히 필요하다.

파일 IO와 네트워크 IO에 집약된 작업이며 동시에 여러 인덱스가 실행할 수 있으므로,

패스트캣에서 멀티 쓰레드로 처리하는 것보다는 장애관리와 성능차원에서 멀티 프로세스로 동작하는 것이 좋다.

## 색인작업절차

1. 패스트캣에 색인 쓰레드가 만들어진다.

2. `java indexer.jar --server.port=<포트번호>` 로 외부 프로세스를 실행한다. 
이때 포트는 남는 포트중 랜덤으로 하나를 결정한다. 도커를 사용한다면, 포트 옵션은 사용하지 않아도 된다.

3. 색인기로 REST API 접근을 하고 READY 상태가 될때까지 대기한다. ` GET /actuator/health` => `{"status":"UP"}`

4. READY가 되면 POST /start 로 색인을 시작한다. 이때 색인할 정보를 전달한다. 향후 path는 달라질수 있다.

5. 시작중일때는 GET /status 로 상태를 확인한다. 현재 어떤 index를 몇건 색인하고 있는지 json으로 전달받는다.

6. 상태가 finished가 되면 색인이 종료된 것이다.

7. 색인이 끝나면 색인기를 명시적으로 종료해준다. spring boot의 actuator를 사용하였으므로, shutdown 명령을 보낼수 있다.
`POST /actuator/shutdown` => `{"message":"Shutting down, bye..."}`

## 사용법

NDJson 파일색인

`POST http://localhost:5005/start`

```json
{
    "scheme": "http",
    "host": "es1.danawa.io",
    "port": 80,
    "index": "song5",
    "type": "ndjson",
    "path": "C:\\Projects\\fastcatx-indexer\\src\\test\\resources\\sample.ndjson",
    "encoding": "utf-8",
    "bulkSize": 1000
}
```
