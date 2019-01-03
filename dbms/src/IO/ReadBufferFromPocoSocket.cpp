#include <Poco/Net/NetException.h>

#include <Common/Exception.h>

#include <IO/ReadBufferFromPocoSocket.h>
#include <Common/NetException.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int CANNOT_READ_FROM_SOCKET;
}


bool ReadBufferFromPocoSocket::nextImpl()
{
    ssize_t bytes_read = 0;

    /// Add more details to exceptions.
    try
    {

        size_t pending =  working_buffer.end() - pos;
        LOG_DEBUG(&Logger::get("ReadBufferFromPocoSocket"),"1pending data :" + std::to_string(pending) );
        bytes_read = socket.impl()->receiveBytes(internal_buffer.begin(), internal_buffer.size());

        LOG_DEBUG(&Logger::get("ReadBufferFromPocoSocket"),"socket read " + std::to_string(bytes_read) + " bytes ");

        size_t pending1 =  working_buffer.end() - pos;
        LOG_DEBUG(&Logger::get("ReadBufferFromPocoSocket"),"2pending data :" + std::to_string(pending1) );


    }
    catch (const Poco::Net::NetException & e)
    {
        throw NetException(e.displayText(), "while reading from socket (" + peer_address.toString() + ")", ErrorCodes::NETWORK_ERROR);
    }
    catch (const Poco::TimeoutException & e)
    {
        throw NetException("Timeout exceeded while reading from socket (" + peer_address.toString() + ")", ErrorCodes::SOCKET_TIMEOUT);
    }
    catch (const Poco::IOException & e)
    {
        throw NetException(e.displayText(), "while reading from socket (" + peer_address.toString() + ")", ErrorCodes::NETWORK_ERROR);
    }

    if (bytes_read < 0)
        throw NetException("Cannot read from socket (" + peer_address.toString() + ")", ErrorCodes::CANNOT_READ_FROM_SOCKET);

    if (bytes_read)
        working_buffer.resize(bytes_read);
    else
        return false;

    return true;
}

ReadBufferFromPocoSocket::ReadBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size)
    : BufferWithOwnMemory<ReadBuffer>(buf_size), socket(socket_), peer_address(socket.peerAddress())
{
}

bool ReadBufferFromPocoSocket::poll(size_t timeout_microseconds)
{
    return offset() != buffer().size() || socket.poll(timeout_microseconds, Poco::Net::Socket::SELECT_READ | Poco::Net::Socket::SELECT_ERROR);
}

}
