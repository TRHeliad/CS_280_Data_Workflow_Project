import numpy as np
import tensorflow as tf
from tensorflow import keras
from keras import layers
import wandb
from wandb.keras import WandbMetricsLogger
import matplotlib.pyplot as plt

# get your data
(x_train, y_train), (x_val, y_val) = keras.datasets.cifar100.load_data()

# constant values
NUM_CLASSES = 100 #100 prediction classes
INPUT_SHAPE = (32,32,3) #shape of the input image 32x32 with 3 channels

# hyperparameters you will be tuning
BATCH_SIZE = 100
EPOCHS = 8
LEARNING_RATE = 3e-4
L1NF = 96
FDROPOUT = 0.25

wandb.init(project = 'Tool Lab 3',
          config={
              'bs':BATCH_SIZE,
              'lr':LEARNING_RATE,
              'epochs': EPOCHS,
              'l1nf':L1NF,
              'fdr':FDROPOUT
          },
           name='RUN 18: EPOCHS 8' #this is your run name, please number your runs 0-n and tell what you changed
          )

# onehot encode your labels so your model knows its a category
y_train = tf.one_hot(y_train,
                     depth=y_train.max() + 1,
                     dtype=tf.float64)
y_val = tf.one_hot(y_val,
                   depth=y_val.max() + 1,
                   dtype=tf.float64)
  
y_train = tf.squeeze(y_train)
y_val = tf.squeeze(y_val)

# here is a basic model that you will add to
model = tf.keras.models.Sequential([
                  
                  # CHANGE THESE: these are layers you should mix up and change
                  layers.Conv2D(64, (2, 2), input_shape = INPUT_SHAPE,
                                activation='relu',
                                padding='same'),
                  layers.MaxPooling2D(2,2),

                  layers.Conv2D(96, (2, 2),
                                activation='relu',
                                padding='same'),
                  layers.MaxPooling2D(2,2),

                  layers.Conv2D(128, (2, 2),
                                activation='relu',
                                padding='same'),
                  layers.MaxPooling2D(2,2),
                  
                  layers.Dropout(FDROPOUT),

                  
                  # DO NOT CHANGE THESE. They should be at the end of your model
                  layers.Flatten(),
                  layers.Dense(NUM_CLASSES, activation='softmax')])


# feel free to experiment with this
model.compile(loss='categorical_crossentropy',
    optimizer=keras.optimizers.RMSprop(learning_rate=LEARNING_RATE),
    # DO NOT CHANGE THE METRIC. This is what you will be judging your model on
    metrics=['accuracy']
)

# Create the model checkpoint callback
checkpointCallbackFunction =  tf.keras.callbacks.ModelCheckpoint(
    filepath='./checkpoints/model.{epoch:02d}-{val_loss:.2f}.h5',
    save_freq="epoch"
)

# here you train the model using some of your hyperparameters and send the data
# to weights and biases after every batch            
history = model.fit(x_train, y_train,
    epochs=EPOCHS,
    batch_size=BATCH_SIZE,
    verbose=1,
    validation_data=(x_val, y_val),
    callbacks=[
        WandbMetricsLogger(log_freq='batch'),
        checkpointCallbackFunction
    ]
)

#This will save the model 
model.save("./checkpoints/trainedModel.h5")