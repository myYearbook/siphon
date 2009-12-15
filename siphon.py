#!/usr/bin/env python
"""
Siphon

Suck all the messages out of Stomp brokers (ActiveMQ) and publish into RabbitMQ (AMQP)

Copyright (c)2009,  Insider Guides, Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
Neither the name of the Insider Guides, Inc. nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

__author__  = "Gavin M. Roy"
__email__   = "gmr@myyearbook.com"
__date__    = "2009-11-25"
__version__ = 0.2

import amqplib.client_0_8 as amqp
import logging
import optparse
import os
import signal
import stomp
import sys
import threading
import time
import yaml
import zlib

threads = []

class AMQP:

    def __init__(self, config):
    
        # Carry the config in the object
        self.config = config
    
        # Connect to RabbitMQ
        self.connection = amqp.Connection( host ='%s:%s' % 
                                           ( self.config['amqp']['connection']['host'], 
                                             self.config['amqp']['connection']['port'] ),
                                           userid = self.config['amqp']['connection']['user'], 
                                           password = self.config['amqp']['connection']['pass'], 
                                           ssl = self.config['amqp']['connection']['ssl'],
                                           virtual_host = self.config['amqp']['connection']['vhost'] )

        # Create our channel
        self.channel = self.connection.channel()
        for queue in self.config['stomp']['queues']:
            
            # Split the queue name up into parameters
            parameters = self.get_exchange_queue_and_routing_key( queue )    
        
            # Create our queue if it doesn't exist        
            logging.debug( 'Creating AMQP Queue "%s"' % parameters['queue'] )
            self.channel.queue_declare( queue = parameters['queue'], 
                                        durable = True, exclusive = False, auto_delete = False )
 
            # Create / Connect to the Exchange
            logging.debug( 'Creating exchange "%s"' % parameters['exchange'] )
            self.channel.exchange_declare( exchange = parameters['exchange'], 
                                           type = 'direct', durable =  True, auto_delete = False )
     
            # Bind to the Queue / Exchange to the routing key we will use
            logging.debug( 'Creating routing key "%s"' % parameters['routing_key'] )
            self.channel.queue_bind( queue = parameters['queue'], 
                                     exchange =  parameters['exchange'], 
                                     routing_key = parameters['routing_key'] )

    def get_exchange_queue_and_routing_key(self, stomp_queue):
    
        # Split the /queue/name format, ignoring the /queue/ part
        parts = stomp_queue.split('/')
        
        """
        We use a exchange.name type format in our stomp queues, try and use our format but gracefully degrade
        and use the default exchange name in the configuration file otherwise
        """
        parts = parts[2].split('.')
        if len(parts) == 1:
            exchange = self.config['amqp']['default_exchange']
            queue = parts[0]
        else:
            exchange = parts[0]
            queue = parts[1]
        
        # Return our dictionary
        return {'exchange': exchange, 'queue': queue, 'routing_key': '%s.%s' % ( exchange, queue ) }    
    
    
    def publish(self, stomp_header, stomp_message):

        # Get the exchange and binding key
        parameters = self.get_exchange_queue_and_routing_key( stomp_header['destination'] )

        # Create our new message to send to RabbitMQ
        logging.debug( 'AMQP publishing to routing_key: "%s"' % parameters['routing_key'] )
        
        if self.config['compress']:
            stomp_message = zlib.compress(stomp_message, self.config['compression_level'])
        
        message = amqp.Message( stomp_message )
        
        if self.config['delivery_mode'] > 0:
            message.properties["delivery_mode"] = self.config['delivery_mode']

        self.channel.basic_publish( message,
                                    exchange = parameters['exchange'],
                                    routing_key = parameters['routing_key'] ) 

    def shutdown(self):
    
        # Close out our AMQP channel and connection
        self.channel.close()
        self.connection.close()

class SiphonThread( threading.Thread ):

    def __init__(self, config, stomp_connection):

        # Set our internal variables    
        self.config = config
        self.shutting_down = False
        self.stomp_connect_tuple = stomp_connection

        self.listener = None
        self.stomp_connection = None
       
        # Init the Thread Object itself
        threading.Thread.__init__(self)        

    def connect(self):

        self.stomp_connection = None
        self.listener = None
        
        # Create our stomp connection
        logging.debug('Connecting to %s:%i' % self.stomp_connect_tuple)
        self.stomp_connection = stomp.Connection( [ self.stomp_connect_tuple ],
                                                  self.config['stomp']['username'],
                                                  self.config['stomp']['password'] )            

        # Start our stomp connection
        self.stomp_connection.start()

        # Connect to our stomp connection
        self.stomp_connection.connect( wait = True )

        # Create our listener object
        self.listener = StompListener( self.config )

        # Provision our callback function
        self.stomp_connection.set_listener('', self.listener )
            
        # Subscribe to the queues we want to dequeue from
        for queue in self.config['stomp']['queues']:
            logging.debug('Subscribing to %s' % queue)
            self.stomp_connection.subscribe( destination = queue, headers = { 'ack': 'auto', 
                                                                              'activemq.dispatchAsync': 'true', 
                                                                              'activemq.prefetchSize': 1
                                                                            } )
                
    def run(self):

        # Connect to our brokers    
        self.connect()

        # Loop while the thread is running
        while not self.shutting_down:
            time.sleep(1)
            
            # If we disconnect
            if not self.stomp_connection.is_connected():

                logging.debug( 'Lost stomp connection to  %s:%i' % self.stomp_connect_tuple )
                time.sleep(1)
                self.connect()


    def shutdown(self):

        logging.debug('Shutting down SiphonThread')
        self.shutting_down = True 

        # Try and shutdown our stomp connection
        try:   
            self.stomp_connection.stop()
        except stomp.internal.exception.NotConnectedException:
            pass

        # Shutdown our listener object
        self.listener.shutdown()

class StompListener(stomp.ConnectionListener):

    def __init__(self, config):
        # Init our AMQP connection
        self.amqp = AMQP(config)
        
        # Counters for stats
        self.errors = 0
        self.messages = 0

    def get_stats(self):
        # Return a dictionary of stats
        return { 'errors': self.errors, 'messages': self.messages }

    def on_error(self, headers, message):
        # Log errors, not sure what we want to do here in the long run
        logging.error( 'Received: %s\n%s' % (headers, message) )
        self.errors += 1
        
    def on_message(self, headers, message):
        # Publish the message received to AMQP
        # logging.debug( 'Received: %s' % headers['message-id'] )
        self.amqp.publish(headers, message)
        self.messages += 1
        
    def shutdown(self):
        # Shutdown our AMQP class / connection
        self.amqp.shutdown()
    
def shutdown():
  
    logging.debug('Stopping all threads.')

    # Shutdown all the threads
    for thread in threads:
        thread.shutdown()

    logging.debug('All threads received the shutdown signal.')   
    sys.exit(0)     

def main():
    """ Main Application Handler """
    
    usage = "usage: %prog [options]"
    version_string = "%%prog %s" % __version__
    description = "%prog requeue daemon"
    
    # Create our parser and setup our command line options
    parser = optparse.OptionParser(usage=usage,
                     version=version_string,
                     description=description)

    
    parser.add_option("-c", "--config", 
                     action="store", type="string", default="siphon.yaml", 
                     help="Specify the configuration file to load.")
    
    parser.add_option("-d", "--detached",
                     action="store_true", dest="detached", default=False,
                     help="Run as a daemon detached from the console.")
    
    parser.add_option("-v", "--verbose",
                     action="store_true", dest="verbose", default=False,
                     help="use debug to stdout instead of logging settings")

    
    # Parse our options and arguments                                                                        
    options, args = parser.parse_args()
    
    # Load the Configuration file
    try:
        stream = file(options.config, 'r')
        config = yaml.load(stream)
        stream.close()
    
    except IOError:
        print "\nError: Invalid or missing configuration file \"%s\"\n" % options.config
        sys.exit(1)
    
    # Set logging levels dictionary
    logging_levels = { 
                        'debug':    logging.DEBUG,
                        'info':     logging.INFO,
                        'warning':  logging.WARNING,
                        'error':    logging.ERROR,
                        'critical': logging.CRITICAL
                     }
    
    # Get the logging value from the dictionary
    logging_level = config['Logging']['level']
    config['Logging']['level'] = logging_levels.get( config['Logging']['level'], 
                                                     logging.NOTSET )

    # If the user says verbose overwrite the settings.
    if options.verbose:
    
        # Set the debugging level to verbose
        config['Logging']['level'] = logging.DEBUG
        
        # If we have specified a file, remove it so logging info goes to stdout
        if config['Logging'].has_key('filename'):
            del config['Logging']['filename']

    else:
        # Build a specific path to our log file
        if config['Logging'].has_key('filename'):
            config['Logging']['filename'] = "%s/logs/%s" % ( 
                                            os.path.dirname(__file__), 
                                            config['Logging']['filename'] )
        
    # Pass in our logging config 
    logging.basicConfig(**config['Logging'])
    logging.info('Log level set to %s' % logging_level)

    # Fork our process to detach if not told to stay in foreground
    if options.detached:
        try:
            pid = os.fork()
            if pid > 0:
                logging.info('Parent process ending.')
                sys.exit(0)                        
        except OSError, e:
            sys.stderr.write("Could not fork: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        
        # Second fork to put into daemon mode
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit from second parent, print eventual PID before
                print 'siphon.py daemon has started - PID # %d.' % pid
                logging.info('Child forked as PID # %d' % pid)
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("Could not fork: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        
        # Let the debugging person know we've forked
        logging.debug( 'siphon.py has forked into the background.' )
        
        # Detach from parent environment
        os.chdir(os.path.dirname(__file__))
        os.setsid()
        os.umask(0) 

        # Close stdin            
        sys.stdin.close()
        
        # Redirect stdout, stderr
        sys.stdout = open(os.path.join(os.path.dirname(__file__), 
                                       'logs/', "stdout.log"), 'w')
        sys.stderr = open(os.path.join(os.path.dirname(__file__), 
                                       'logs/', "stderr.log"), 'w')
                                                 
    # Set our signal handler so we can gracefully shutdown
    signal.signal(signal.SIGTERM, shutdown)

    brokers = []

    # Loop through our config and kick off all of our threads
    for broker in config['Siphon']['stomp']['brokers']:
    
        # Break out the host and port
        host, port = broker.split(':')
    
        broker_dict = { 'broker': broker }
        broker_dict['threads'] = []
    
        # Kick of the number of threads per broker
        for y in xrange(0, config['Siphon']['stomp']['threads_per_broker']):
            logging.debug( 'Kicking off thread #%i for %s:%s' % ( y, host, port ) )
            
            new_thread = SiphonThread( config['Siphon'], ( host, int(port) ) )
            broker_dict['threads'].append( new_thread )
            new_thread.start()
    
    # Loop until someone wants us to stop
    while 1:
        
        try:
                
            # Sleep is so much more CPU friendly than pass
            time.sleep(1)
        
        except (KeyboardInterrupt, SystemExit):
            # The user has sent a kill or ctrl-c
            shutdown()
        
# Only execute the code if invoked as an application
if __name__ == '__main__':
    
    # Run the main function
    main()
