package com.danawa.fastcatx.indexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@RestController
@RequestMapping("/")
public class CommandController {
    private static Logger logger = LoggerFactory.getLogger(CommandController.class);

    @GetMapping(value = "/")
    @CrossOrigin("*")
    public ResponseEntity<?> getDefault() {

        String responseBody = "FastcatX Indexer!";
        return new ResponseEntity<>(responseBody, HttpStatus.OK);
    }

    @GetMapping(value = "/status")
    @CrossOrigin("*")
    public ResponseEntity<?> getStatus(HttpServletRequest request,
                                   @RequestParam Map<String,String> queryStringMap,
                                   @RequestBody(required = false) byte[] body) {

        String responseBody = "";
        return new ResponseEntity<>(responseBody, HttpStatus.OK);
    }

    @PutMapping(value = "/start")
    @CrossOrigin("*")
    public ResponseEntity<?> doStart(HttpServletRequest request,
                                       @RequestParam Map<String,String> queryStringMap,
                                       @RequestBody(required = false) byte[] body) {

        String responseBody = "";
        return new ResponseEntity<>(responseBody, HttpStatus.OK);
    }

    @PutMapping(value = "/stop")
    @CrossOrigin("*")
    public ResponseEntity<?> doStop(HttpServletRequest request) {

        String responseBody = "";
        return new ResponseEntity<>(responseBody, HttpStatus.OK);
    }
}
