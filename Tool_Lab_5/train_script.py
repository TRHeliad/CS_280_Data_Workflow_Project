import numpy as np
import tensorflow as tf
from tensorflow import keras
from keras import layers
import matplotlib.pyplot as plt
import sys

# import the dataset from the current directory
dataset = np.load('./cifar100.npz')
x_train = dataset['x_train']
y_train = dataset['y_train']
x_val = dataset['x_val']
y_val = dataset['y_val']

# constant values
NUM_CLASSES = 100 #100 prediction classes
INPUT_SHAPE = (32,32,3) #shape of the input image 32x32 with 3 channels

# hyperparameters you will be tuning
path, BATCH_SIZE, EPOCHS, LEARNING_RATE, L1NF, FDROPOUT = sys.argv
BATCH_SIZE = int(BATCH_SIZE)
EPOCHS = int(EPOCHS)
LEARNING_RATE = float(LEARNING_RATE)
L1NF = int(L1NF)
FDROPOUT = float(FDROPOUT)

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
    layers.Conv2D(L1NF, (3, 3), input_shape = INPUT_SHAPE,
                activation='relu',
                padding='same'),
    layers.MaxPooling2D(2,2),
    layers.Dropout(FDROPOUT),

    # DO NOT CHANGE THESE. They should be at the end of your model
    layers.Flatten(),
    layers.Dense(NUM_CLASSES, activation='softmax')]
)


# feel free to experiment with this
model.compile(loss='categorical_crossentropy',
    optimizer=keras.optimizers.RMSprop(learning_rate=LEARNING_RATE),
    # DO NOT CHANGE THE METRIC. This is what you will be judging your model on
    metrics=['accuracy']
)

# here you train the model using some of your hyperparameters and send the data
# to weights and biases after every batch
history = model.fit(x_train, y_train,
    epochs=EPOCHS,
    batch_size=BATCH_SIZE,
    verbose=1,
    validation_data=(x_val, y_val)
)

val_accuracy = model.evaluate(x_val, y_val)[1]

file_name = f"{BATCH_SIZE}-{EPOCHS}-{LEARNING_RATE}-{L1NF}-{FDROPOUT}"

#This will save the model
model.save(f"./models/{file_name}.h5")

with open(f"./results/{file_name}.txt", "w") as output:
    output.write(str(val_accuracy))
