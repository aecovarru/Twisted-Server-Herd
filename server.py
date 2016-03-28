import time
import logging
import sys
import json

from twisted.internet import protocol, reactor
from twisted.protocols.basic import LineReceiver
from twisted.python import log
from twisted.web.client import getPage
from twisted.application import service, internet

# Google API Setup
GOOGLE_PLACE_API_KEY = "AIzaSyDDe3xXxav1GZEMJ4sl17l0zzqYqMxOP8Y"
GOOGLE_PLACE_API_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?"

# Which servers talk to one another
talks_to = {
    "Alford" :   ["Parker", "Welsh"],
    "Bolden" :   ["Parker", "Welsh"],
    "Hamilton" : ["Parker"],
    "Parker" :   ["Alford", "Bolden", "Hamilton"],
    "Welsh" :   ["Alford", "Bolden"]
}

# address of every server
server_address = {
    "Alford" :   {"ip":"localhost", "port": 12490},
    "Bolden" :   {"ip":"localhost", "port": 12491},
    "Hamilton" : {"ip":"localhost", "port": 12492},
    "Parker" :   {"ip":"localhost", "port": 12493},
    "Welsh" :   {"ip":"localhost", "port": 12494}
}

class ProxyServerProtocol(LineReceiver):

    def __init__(self, factory):
        self.factory = factory

    def connectionMade(self):
        logging.info("Connection established.")

    def lineReceived(self, line):

        parse_list = line.split()
        if len(parse_list) < 1:
            self.invalidCommand(line, "response missing values.")
            return

        # respond according to the first element in the parse list
        if parse_list[0] == "IAMAT":
            self.respondIAMAT(line)
        elif parse_list[0] == "AT":
            self.respondAT(line)
        elif parse_list[0] == "WHATSAT":
            self.respondWHATSAT(line)
        # invalid value in first element
        else:
            self.invalidCommand(line, "response name invalid.")
        return

    def invalidCommand(self, line, append=""):
        logging.info("Invalid Command: " + line + " " + append)
        self.transport.write("? " + line + "\n")
        return

    def respondIAMAT(self, line):
        parse_list = line.split()
        if len(parse_list) != 4:
            self.invalidCommand(line)
            return

        client_id = parse_list[1]
        client_pos = parse_list[2]
        client_time = parse_list[3]

        # find difference in time sent and time received
        try:
            time_diff = time.time() - float(client_time)
        except Exception, e:
            self.invalidCommand(line, "Invalid time paramter.")
            return

        # Make response and check for negative time values due to time schew
        if time_diff >= 0:
            response = "AT {0} +{1} {2}".format(self.factory.name, time_diff, ' '.join(parse_list[1:]))
        else:
            response = "AT {0} {1} {2}".format(self.factory.name, time_diff, ' '.join(parse_list[1:]))

        # Respond to IAMAT message
        self.transport.write(response + "\n")
        logging.info("Successfully responded to IAMAT using " + response)

        # Check for outdated AT message
        if (client_id in self.factory.clients) and (client_time <= self.factory.clients[client_id]["time"]):
            logging.info("Outdated position info " + line)
            return

        # save location info on this server
        self.factory.clients[client_id] = {"response": response, "time": client_time}
        logging.info("Updated {0} : {1}".format(client_id, self.factory.clients[client_id]["response"]))

        # send info to all other servers
        self.flood(response)
        return

    # Respond to an AT message from a client caused by a server flood, clients won't send these independently
    def respondAT(self, line):
        parse_list = line.split()
        if len(parse_list) != 7:
            self.invalidCommand(line, "Invalid AT message from server.")
            return

        client_id = parse_list[3]
        client_time = parse_list[5]
        sender = parse_list[6]


        # check if AT message is outdated to stop flooding
        if (client_id in self.factory.clients) and (client_time <= self.factory.clients[client_id]["time"]):
            logging.info("Outdated AT info: " + line)
            return

        # store response and time info
        self.factory.clients[client_id] = {"response": ' '.join(parse_list[:-1]), "time": client_time}
        logging.info("Updated {0} : {1}".format(client_id, self.factory.clients[client_id]["response"]))

        self.flood(self.factory.clients[client_id]["response"], sender)
        return



    def respondWHATSAT(self, line):
        parse_list = line.split()
        if len(parse_list) != 4:
            self.invalidCommand(line)
            return

        try:
            client_id = parse_list[1]
            radius = int(parse_list[2])
            upper_bound = int(parse_list[3])
        except Exception, e:
            self.invalidCommand("invalid input radius or upper bound")
            return


        # check bounds of radius and upper bound of Google place search
        if radius > 50 or upper_bound > 20:
            self.invalidCommand(line)


        # make sure the client id has sent its position before, otherwise the WHATSAT command is invalid
        if not (client_id in self.factory.clients):
            self.invalidCommand(line)
            return

        # get message from dictionary
        at_message = self.factory.clients[client_id]["response"]

        try:
            client_position = at_message.split()[4]
            position_query = client_position.replace('+', ' +').replace('-', ' -').strip().replace(' ', ',')
            google_query = "{0}location={1}&radius={2}&key={3}".format(GOOGLE_PLACE_API_URL, position_query, str(radius), GOOGLE_PLACE_API_KEY)
            logging.info("Google places query: {0} sent".format(google_query))
            print "HERE"
            google_response = getPage(google_query)
            # create callback for when getPage returns the google response asynchronously
            print "MAYBE"
            callback = lambda response:(self.process_successful_google_query(response, upper_bound, at_message, google_query))
            google_response.addCallback(callback)
            print "HERE?"
        except Exception, e:
            logging.error("Google API error")


    def process_successful_google_query(self, google_response, upper_bound, at_message, google_query):
        logging.info("Google places API response: " + google_response)
        json_object = json.loads(google_response)
        results = json_object["results"]
        message = "{0}\n{1}\n\n".format(at_message, json.dumps(json_object, indent=4))
        self.transport.write(message)
        logging.info("Respond to WHATSAT message with " + at_message + " and queried Google with " + google_query)
    
    def flood(self, line, sentfrom = ''):
        # Append server name to AT message
        message = line + ' ' + self.factory.name
        for server in talks_to[self.factory.name]:
            # Make connection every time vs not
            if server != sentfrom:
                if server in self.factory.connected_servers:
                    self.factory.connected_servers[server].send_at_message(message)
                    logging.info("Location update sent from {0} to {1}".format(self.factory.name, server))
                else:
                    reactor.connectTCP(server_address[server]["ip"], server_address[server]["port"], ProxyClientFactory(self.factory, server, message))
                    logging.info("Location update sent from {0} to {1}".format(self.factory.name, server))
        return


class ProxyServerFactory(protocol.ServerFactory):
    
    def __init__(self, name, port):
        # info
        self.name = name
        self.port = port
        self.clients = {}
        self.connected_servers = {}
        self.log_file = "server-" + self.name + ".log"
        logging.basicConfig(filename = self.log_file, level = logging.DEBUG, filemode = 'a')
        logging.info('Server {0}:{1} started'.format(self.name, self.port))

    def buildProtocol(self, addr):
        return ProxyServerProtocol(self)

    def stopFactory(self):
        logging.info("{0} server shutdown".format(self.name))



class ProxyClientProtocol(LineReceiver):
    def __init__ (self, factory):
        self.factory = factory

    def connectionMade(self):
        # connect server with client factory in connected_server dictionary
        self.factory.server_object.connected_servers[self.factory.server_send] = self.factory
        logging.info("Connection from client: {0} to server: {1} established.".format(self.factory.server_object.name, self.factory.server_send))
        # first 
        self.sendLine(self.factory.at_message)

    def connectionLost(self, reason):
        # it seems that connectionLost is called before clientConnectionLost
        if self.factory.server_send in self.factory.server_object.connected_servers:
            del self.factory.server_object.connected_servers[self.factory.server_send]
            logging.info("Connection from client: {0} to server: {1} lost.".format(self.factory.server_object.name, self.factory.server_send)) 
        return

class ProxyClientFactory(protocol.ClientFactory):
    def __init__(self, server_object, server_send, at_message):
        self.server_object = server_object
        self.server_send = server_send
        self.at_message = at_message
        return

    def buildProtocol(self, addr):
        self.protocol = ProxyClientProtocol(self)
        return self.protocol

    def send_at_message(self, at_message):
        # send message when server_object requests
        self.protocol.sendLine(at_message)
        return
  

    def clientConnectionLost(self, connector, reason):
        # delete yourself from the connected_server dictionary in the server_object
        if self.server_send in self.server_object.connected_servers:
            del self.server_object.connected_servers[self.server_send]
            logging.info("Connection lost from client {0} to server: {1}.".format(self.server_object.name, self.server_send))
        return

    def clientConnectionFailed(self, connector, reason):
        logging.info("Connection failed from client: {0} to server {1}.".format(self.server_object.name, self.server_send))
        return




def main():
    if len(sys.argv) != 2:
        print "Usage: python server.py server name. Choose from the server names Alford, Bolden, Hamilton, Parker and Welsh.\n"
        exit()


    server_name = sys.argv[1]
    try:
        if server_name in server_address:
            server_factory = ProxyServerFactory(server_name, server_address[server_name]["port"])
            reactor.listenTCP(server_address[server_name]["port"], server_factory)
            reactor.run()
        else:
            print "Error: server name not recognized from configuration, Choose from the server names Alford, Bolden, Hamilton, Parker and Welsh\n"
    except Exception, e:
        print "Error"


if __name__ == '__main__':
    main()
