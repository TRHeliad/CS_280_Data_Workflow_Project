from fastapi import FastAPI, File, UploadFile
from slowapi.errors import RateLimitExceeded
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from starlette.requests import Request
from starlette.responses import Response

from tensorflow import keras

from PIL import Image
from io import BytesIO
import numpy as np
import cv2

import pickle

def unpickle(file):
    with open(file, 'rb') as fo:
        myDict = pickle.load(fo, encoding='latin1')
    return myDict

metaData = unpickle("meta")
fine_labels = metaData["fine_label_names"]

#This will load my model
model = keras.models.load_model("./checkpoints/trainedModel.h5")

limiter = Limiter(key_func=get_remote_address) # Gets the user's address
app = FastAPI()
app.state.limiter = limiter # Define's the api's limiter object
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler) # Adds the exception to the api

@app.get("/base/home/")
@limiter.limit("15/minute") #Defines the limit for this specific request
async def homepage(request: Request, response: Response):
  return {"message": "Ben - Madsen's CS280 Image Classifier"}


# This is a post request that allows you to receive a file:
@app.post("/classify/")
@limiter.limit("5/minute") #Defines the limit for this specific request
async def classify(request:Request, response:Response, file: UploadFile = File(...)):
    myFileInBytes = await file.read() # Reads file information from request as a stream of bytes

    #myFileInBytes = open(myFileString) # This is how you'll receive your image from the request.
    image = Image.open(BytesIO(myFileInBytes)) # Converts file to a python image from a stream of bytes.
    imageArray = np.asarray(image) #converts image to numpy array
    smallImage = cv2.resize(imageArray, dsize=(32, 32), interpolation=cv2.INTER_CUBIC) # Resizes the image
    batchedImage = np.expand_dims(smallImage, axis=0)

    result = model.predict(batchedImage)
    label = fine_labels[np.argmax(result, axis=1)[0]]

    return {"classification": label} # Returns the filename as a response.


