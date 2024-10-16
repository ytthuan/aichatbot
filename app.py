#how to use custom data layer
#from main file like app.py init the data layer
import chainlit.data as cl_data
from your_custom_datalayer_files import MyMongoDbDataLayer

cl_data._data_layer = MyMongoDbDataLayer(mongodb_uri=getenv("MONGODB_URI")) 

# normal chainlit code
