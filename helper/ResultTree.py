import os
import csv
import collections
import fnmatch
import copy
import numpy
import itertools
import sys

def isingle(item):
	"iterator that yields only a single value then stops, for chaining"
	yield item

class ResultTree(object):
			
	def __init__(self, name, file_types="*.json", delimiter=";", level=0):
		self.name = name
		self.children = []
		self.parent = None
		self.file_types = file_types
		self.level = level
		self.is_leaf = False
		self.result_dict = None
		self.delimiter = delimiter

	def __iter__(self):
		return itertools.chain(isingle(self), *map(iter, self.children))

	# Iterates recursively through result directory and
	# creates a tree of result objects
	def parse(self):
		if os.path.isdir(self.name):
			directory = self.name
			for f in os.listdir(directory):
				path = os.path.join(directory, f)
				if os.path.isfile(path) and len(fnmatch.filter([path], self.file_types)) == 0:
					continue
				r = ResultTree(path, file_types=self.file_types, level=self.level+1)
				r.parent = self
				self.children.append(r)
				r.parse()
		else:
			result_file = self.name
			self.parse_result_csv(result_file)
		return self

	# Parses result CSV file, assumes header line as first line of file
	def parse_result_csv(self, filename):
		self.result_dict = collections.defaultdict(list)
		self.is_leaf = True
		with open(filename) as f:
			reader = csv.DictReader(f, delimiter=self.delimiter, quotechar='|', quoting=csv.QUOTE_MINIMAL)
			for row in reader:
				for (k,v) in row.items():
					try:
						value = int(v)
					except Exception, e:
						try:
							value = float(v)
						except Exception, e:
							value = v
					self.result_dict[k].append(value)
		# for key in result_dict:
		# 	path = os.path.join(filename, key)
		# 	r = ResultTree(path, file_types=self.file_types, level=self.level+1)
		# 	r.parent = self
		# 	r.values = result_dict[key]
		# 	r.is_leaf = True
		# 	self.children.append(r)

	# filters result hierarchy and only keeps levels from levels
	# leafs specifies levels that have to be leaves
	# filters specifies list of filters. format: {level1: filter_string, level2: filter_string}.
	# if filter ist string, filter is applied to all levels
	def filter(self, levels, leafs = [], filters="*"):
		filter_result = self.shallow_copy()
		last_inserted_node = filter_result

		if isinstance(filters, str):
			# apply filter to all levels
			filters = {-1: filters} 

		for node in self:
			if node == self:
				continue
			if -1 in filters.keys() and len(fnmatch.filter([node.name], filters[-1])) == 0:
				continue
			if node.level in filters.keys() and len(fnmatch.filter([node.name], filters[node.level])) == 0:
				continue

			if node.level in levels and (node.level not in leafs or node.is_leaf):
				if node.level > last_inserted_node.level:
					# going down in the hierachy, remember parent
					last_inserted_parent = last_inserted_node
				if node.level < last_inserted_node.level:
					# going up in the hierachy, find correct parent
					while last_inserted_parent.level >= node.level:
						last_inserted_parent = last_inserted_parent.parent
				node_copy = node.shallow_copy()
				node_copy.parent = last_inserted_parent
				last_inserted_parent.children.append(node_copy)
				last_inserted_node = node_copy
		return filter_result

	# unifies specified levels of tree to one node
	def aggregate(self, levels):
		aggregate_result = copy.deepcopy(self)
		for level in levels:
			nodes = collections.defaultdict(list)
			for node in aggregate_result:
				if node.level == level and node.is_leaf:
					nodes[node.parent].append(node)
			
			for key, value in nodes.items():
				merged_node = self.merge_nodes(value)
				key.children = [merged_node]
		return aggregate_result

	def merge_nodes(self, node_list):
		if len(node_list) == 0:
			return None
		merged_node = copy.copy(node_list[0])
		merged_node.children = []
		merged_node.result_dict = collections.defaultdict(list)

		# check keys and counts
		reference_node = node_list[0]
		for node in node_list:
			if sorted(reference_node.result_dict.keys()) != sorted(node.result_dict.keys()):
				raise Exception("Cannot merge nodes, keys do not match: " + node.name)
			count = -1
			for key in node.result_dict.keys():
				if count > 0 and count != len(node.result_dict[key]):
					raise Exception("Cannot merge nodes, row counts are not the same: " + node.name)

		for node in node_list:
			merged_node.children.extend(node.children)
			for key in node.result_dict.keys():
				values = node.result_dict[key]
				merged_node.result_dict[key].extend(values)
		return merged_node

	def get_key_value(self, filter, x, y):
		result = {}
		for node in self:
			if filter(node) == True:
				x_val = x(node)
				y_val = y(node)
				result[x_val] = y_val
		return collections.OrderedDict(sorted(result.items(), key=lambda t: t[0]))

	def get_values(self, filter, y):
		result = []
		for node in self:
			if filter(node) == True:
				y_val = y(node)
				result.append(y_val)
		return result

	# @property
	# def average(self):
	# 	return numpy.average(self.values)

	# @property
	# def median(self):
	# 	return numpy.median(self.values)

	# @property
	# def min(self):
	# 	return numpy.min(self.values)

	# @property
	# def max(self):
	# 	return numpy.max(self.values)

	# @property
	# def std(self):
	# 	return numpy.std(self.values)
	
	# @property
	# def count(self):
	# 	return len(self.values)
		


	def print_info(self, with_values=False, with_stats=False):
		for node in self:
			node.print_indentation("+--")
			sys.stdout.write(str(node.level))
			sys.stdout.write(str(node.name))
			sys.stdout.write("\n")
			if with_values and node.is_leaf:
			 	# node.print_attribute("values")
			 	print node.result_dict
			if with_stats and node.is_leaf:
				keys = node.result_dict.keys()
				node.print_indentation("|__ Keys: ")
				sys.stdout.write(str(keys))
				if len(keys) > 0:
					sys.stdout.write(", Count: " + str(len(node.result_dict[keys[0]])))
				sys.stdout.write("\n")

				# node.print_attribute("average")
				# node.print_attribute("median")
				# node.print_attribute("min")
				# node.print_attribute("max")
				# node.print_attribute("std")
				# node.print_attribute("count")

	def print_indentation(self, s):
		for x in range(self.calc_level()):
			sys.stdout.write("|  ")
		sys.stdout.write(s)

	def print_attribute(self, attribute_name):
		self.print_indentation("|__ " + attribute_name + ": ")
		sys.stdout.write(str(getattr(self, attribute_name)))
		sys.stdout.write("\n")
	
	def calc_level(self):
		level = 0
		parent = self.parent
		while parent != None:
			level = level + 1
			parent = parent.parent
		return level

	def shallow_copy(self):
		c = copy.copy(self)
		c.children = []
		c.parent = None
		return c


