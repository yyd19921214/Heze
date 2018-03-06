package com.yudy.heze.server;

import com.yudy.heze.network.Message;

public interface RequestHandler {

    short PING = 1;
    short UUID = 2;
    short FETCH = 3;
    short PRODUCER = 4;
    short REPLICA = 5;

    Message handler(Message request);


}
