from __future__ import absolute_import

from datetime import datetime
import logging
import sys

from volttron.platform.vip.agent import Agent, Core, PubSub, compat
from volttron.platform.agent import utils
from volttron.platform.messaging import headers as headers_mod

from . import settings
import numpy as np 
import operator

utils.setup_logging()
_log = logging.getLogger(__name__)


class UtilityAgent(Agent):
	'''The Utility Agent is a broker on behalf of a energy distribution company.  It has a
	   function describing the cost of energy as a function of the amount distributed. Setting
	   the maximum value of the supply available will act as a upper-bound on the amount that
	   can be provided to customers.
	   End users, in the form of "homeowner agents" can bid for an allocation of the available
	   energy. This process uses a single bidding round where each homeowner provides a bid curve
	   (pricing willing to pay vs quantity willing to buy). These curves are aggregated and then
	   matched against the supply curve to produce a clearning price.  This clearing price is
	   returned to each homeowner as the operating constraint for the effective period.
	   
	   To change the bidding period, change the HEARTBEAT_PERIOD in settings.py
	   
	   Intwine Connect, LLC
	   October, 2016
	'''
	def __init__(self, config_path, **kwargs):
		super(UtilityAgent, self).__init__(**kwargs)
		self.config = utils.load_config(config_path)
		self._agent_id = self.config['agentid']
		
		# configure the Utility Agent's supply curve
		# using a fixed curve here, but could easily be moved to a external source
		self.price=range(1,6)
		self.quantity=range(1,6)
		
		# init and configure number of expected homeowner agents that will be bidding
		self.hwA_curves={}
		self.number_of_hwA=1


	@Core.receiver('onsetup')
	def setup(self, sender, **kwargs):
		self._agent_id = self.config['agentid']
		print(self._agent_id) 

		
	#combine all curves of homeownerAgents into a "demand curve"
	def combine_curves(self,hwA_curves):
		#append all price lists of homeownerAgents to temp_price
		temp_price=[]
		for key, value in hwA_curves.items():
			temp_price+=value[0]
		#remove duplicate prices
		temp_price = list(set(temp_price))
		temp_price.sort()
		
		#All items of quantity are initialized 0 
		temp_quantity=[0]*len(temp_price)
		for key, value in hwA_curves.items():
			for i in range(len(value[1])):
				'''For each item of quantity, find its index/position in temp_quantity and accumulate 
				according to its corresponding price
				'''
				temp_quantity[temp_price.index(value[0][i])]+=value[1][i]
		#combined curve
		return temp_price,temp_quantity


	#Find the intersection/clearing price between "demand curve" and "supply curve" in "quantity-price" coordinate
	def compute_clearing_price(self,hwA_curves):
		bid_price,bid_quantity=self.combine_curves(hwA_curves)
   
		'''If the bid quantity extends the supply quantity, 
		   the supply curve will keep horizontal in the maximized quantity 
		   no matter how much price
		'''
		bid_quantity.sort()
		bid_price.sort(reverse=True)
		
		offer_x = self.quantity
		offer_y = self.price
		bid_x = bid_quantity
		bid_y = bid_price
		# verify that there is a possible intersection
		print bid_x, bid_y, bid_y[0]
		print offer_x, offer_y, offer_y[-1]
		if True: #offer_y[-1] <= bid_y[0]:
			# get list of the unique points in the offer and bid
			x = list(set(offer_x + bid_x))
			
			# linearly interpolate the offer and bid curves wrt to the new x-axis
			offer_y = np.interp(x, offer_x, offer_y)
			bid_y = np.interp(x, bid_x, bid_y)
			
			# find the zero crossing of the difference between the offer and bid
			delta = map(operator.sub, offer_y, bid_y)
			if 0.0 in delta:
				qty = x[delta.index(0.0)]
			else:
				zero_index = next(x for x in delta if x >= 0.0)
				zero_index = delta.index(zero_index)
				qty = np.interp(0.0, delta[zero_index-1:zero_index+1],x[zero_index-1:zero_index+1])
			
			# compute the MCP based on the zero-crossing
			clearing_price = np.interp(qty, x, offer_y)
			clearing_quantity = qty
			revenue = 0
			
		else:
			clearing_price = 99999
			clearing_quantity = 0
			revenue = 0
		
		return clearing_price,clearing_quantity,revenue


	@PubSub.subscribe('pubsub', '')
	def on_match(self, peer, sender, bus,  topic, headers, message):
		#Use match_all to receive all messages and print them out.
		if sender == 'pubsub.compat':
			message = compat.unpack_legacy_message(headers, message)
		if topic=="Bidding":		   
			_log.info("Topic: %r, from, %r, Demand curve: %r",topic, headers.get('AgentID'), message)

			'''Curve Dictionary of Homeowner Agents
			{AgentID: Bidding Price, Bidding Quantity}
			'''
			self.hwA_curves[headers.get('AgentID')]=[message[1],message[3]]
			
			#Wait until getting all bidding/demand curves
			if len(self.hwA_curves)==self.number_of_hwA:
				clearing_price,clearing_quantity,revenue=self.compute_clearing_price(self.hwA_curves)
				message1 = [clearing_price, clearing_quantity]
				self.vip.pubsub.publish('pubsub', 'clearing price', headers, message1)
				_log.info('clearing price is: %r, clearing quantity is %r,  revenue: %r', clearing_price,clearing_quantity, revenue)
			else:
				_log.info('Waiting for bidding from homeownerAgent...')
		elif (topic == "Load Status"):
			_log.info("message = %r", message)


	@Core.periodic(settings.HEARTBEAT_PERIOD)
	def publish_heartbeat(self):
		'''Send "request for bid" message every HEARTBEAT_PERIOD seconds.
		   HEARTBEAT_PERIOD is set and can be adjusted in the settings module.
		'''
		now = datetime.utcnow().isoformat(' ') + 'Z'
		headers = {
			'AgentID': self._agent_id,
			headers_mod.CONTENT_TYPE: headers_mod.CONTENT_TYPE.PLAIN_TEXT,
			headers_mod.DATE: now,
		}
		
		self.vip.pubsub.publish('pubsub', 'request for bids', headers, now)
		_log.info('Request for bids ')
		_log.info('Supply curve: y=x,x in [1,6]')
		_log.info('Waiting for bidding from homeownerAgent...')

		
def main(argv=sys.argv):
	'''Main method called by the eggsecutable.'''
	try:
		utils.vip_main(UtilityAgent)
	except Exception as e:
		_log.exception('unhandled exception')


if __name__ == '__main__':
	# Entry point for script
	sys.exit(main())