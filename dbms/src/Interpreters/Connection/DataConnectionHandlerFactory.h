//
// Created by jungle on 19-6-17.
//

#pragma  once

namespace DB {



    class DataConnectionHandlerFactory : public Poco::Net::TCPServerConnectionFactory {

    private:

        Poco::Logger * log;

    public:
        explicit DataConnectionHandlerFactory(ExechangeNode & node_, bool secure_ = false)
                : node(node_)
                , log(&Logger::get(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory"))
        {
        }

        Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
        {
            LOG_TRACE(log,
                      "TCP Request. "
                              << "Address: "
                              << socket.peerAddress().toString());

            //return new TCPHandler(server, socket);
            //return  new ExchangeNodeTcpHandler(node,socket);
            DataConnectionHandler * handler = new DataConnectionHandler(socket);

            //node.addHandler(std::shared_ptr<ExchangeNodeTcpHandler>(handler));

            return  handler;
        }


    };



}