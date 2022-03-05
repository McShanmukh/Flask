from couchbase.cluster import Cluster, ClusterOptions, QueryOptions
from couchbase_core.cluster import PasswordAuthenticator
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, Boolean
from sqlalchemy.schema import CreateSchema


cluster = Cluster('http://10.2.112.251:8091/', ClusterOptions(PasswordAuthenticator('dbadmin','cb#admindev')))