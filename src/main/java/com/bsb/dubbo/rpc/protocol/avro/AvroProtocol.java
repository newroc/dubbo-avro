package com.bsb.dubbo.rpc.protocol.avro;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.protocol.AbstractProxyProtocol;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.reflect.ReflectRequestor;
import org.apache.avro.ipc.reflect.ReflectResponder;

import java.net.InetSocketAddress;


/**
 * Created with IntelliJ IDEA.
 * User: Newroc
 * Date: 13-6-15
 * Time: 下午10:19
 * To change this template use File | Settings | File Templates.
 */
public class AvroProtocol extends AbstractProxyProtocol {
    public static final int DEFAULT_PORT = 40881;
    private static final Logger logger = LoggerFactory.getLogger(AvroProtocol.class);
    public static final String NAME = "avro";

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    protected <T> Runnable doExport(T impl, Class<T> type, URL url)
            throws RpcException {
        final Server server = new NettyServer(new ReflectResponder(type, impl), new InetSocketAddress(url.getHost(), url.getPort()));
        AvroProtocol.logger.info("Start Avro Server");
        server.start();

        return new Runnable() {
            public void run() {
                try {
                    AvroProtocol.logger.info("Close Avro Server");
                    server.close();
                } catch (Throwable e) {
                    AvroProtocol.logger.warn(e.getMessage(), e);
                }
            }
        };
    }

    @Override
    protected <T> T doRefer(Class<T> type, URL url) throws RpcException {
        try {
            NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(url.getHost(), url.getPort()));
            // client code - attach to the server and send a message
            T ref = ReflectRequestor.getClient(type, client);
            logger.info("Create Avro Client");
            return ref;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RpcException("Fail to create remoting client for service(" + url + "): " + e.getMessage(), e);
        }
    }

}
