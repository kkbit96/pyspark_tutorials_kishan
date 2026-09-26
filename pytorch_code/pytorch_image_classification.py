"""
PyTorch Image Classification Example
A simple image classification example using MNIST dataset
"""

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import matplotlib.pyplot as plt
import numpy as np

print("=" * 50)
print("PyTorch Image Classification Example")
print("=" * 50)

# Set random seed for reproducibility
torch.manual_seed(42)
np.random.seed(42)

# 1. Generate synthetic image data (28x28 grayscale images like MNIST)
print("\n1. Generating synthetic image data...")
num_samples = 1000
image_size = 28 * 28  # 28x28 pixels
num_classes = 10

# Generate random images and labels
X_images = torch.randn(num_samples, image_size)
y_labels = torch.randint(0, num_classes, (num_samples,))

# Split into train and test sets
train_size = int(0.8 * num_samples)
X_train, X_test = X_images[:train_size], X_images[train_size:]
y_train, y_test = y_labels[:train_size], y_labels[train_size:]

print(f"Training samples: {len(X_train)}")
print(f"Test samples: {len(X_test)}")
print(f"Image size: {image_size}")
print(f"Number of classes: {num_classes}")

# 2. Create DataLoader
print("\n2. Creating DataLoaders...")
train_dataset = TensorDataset(X_train, y_train)
test_dataset = TensorDataset(X_test, y_test)

train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)
test_loader = DataLoader(test_dataset, batch_size=32, shuffle=False)

# 3. Define the Neural Network
print("\n3. Defining the Neural Network...")

class ImageClassifier(nn.Module):
    def __init__(self, input_size, hidden_size, num_classes):
        super(ImageClassifier, self).__init__()
        self.network = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(hidden_size, hidden_size // 2),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(hidden_size // 2, num_classes)
        )
    
    def forward(self, x):
        return self.network(x)

input_size = image_size
hidden_size = 256
model = ImageClassifier(input_size, hidden_size, num_classes)

print(f"Model architecture:\n{model}")

# 4. Loss function and optimizer
criterion = nn.CrossEntropyLoss()
optimizer = optim.Adam(model.parameters(), lr=0.001)

# 5. Training function
def train_model(model, train_loader, criterion, optimizer, epochs):
    model.train()
    train_losses = []
    train_accuracies = []
    
    for epoch in range(epochs):
        running_loss = 0.0
        correct = 0
        total = 0
        
        for images, labels in train_loader:
            # Forward pass
            outputs = model(images)
            loss = criterion(outputs, labels)
            
            # Backward pass and optimize
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            
            # Statistics
            running_loss += loss.item()
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()
        
        epoch_loss = running_loss / len(train_loader)
        epoch_acc = 100 * correct / total
        train_losses.append(epoch_loss)
        train_accuracies.append(epoch_acc)
        
        if (epoch + 1) % 10 == 0:
            print(f'Epoch [{epoch+1}/{epochs}], Loss: {epoch_loss:.4f}, Accuracy: {epoch_acc:.2f}%')
    
    return train_losses, train_accuracies

# 6. Evaluation function
def evaluate_model(model, test_loader, criterion):
    model.eval()
    test_loss = 0.0
    correct = 0
    total = 0
    
    with torch.no_grad():
        for images, labels in test_loader:
            outputs = model(images)
            loss = criterion(outputs, labels)
            
            test_loss += loss.item()
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()
    
    test_loss = test_loss / len(test_loader)
    test_acc = 100 * correct / total
    
    return test_loss, test_acc

# 7. Train the model
print("\n4. Training the model...")
epochs = 50
train_losses, train_accuracies = train_model(model, train_loader, criterion, optimizer, epochs)

# 8. Evaluate the model
print("\n5. Evaluating the model...")
test_loss, test_accuracy = evaluate_model(model, test_loader, criterion)
print(f'Test Loss: {test_loss:.4f}')
print(f'Test Accuracy: {test_accuracy:.2f}%')

# 9. Visualize training progress
print("\n6. Creating training visualization...")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# Plot training loss
ax1.plot(train_losses, label='Training Loss')
ax1.set_xlabel('Epoch')
ax1.set_ylabel('Loss')
ax1.set_title('Training Loss over Epochs')
ax1.legend()
ax1.grid(True)

# Plot training accuracy
ax2.plot(train_accuracies, label='Training Accuracy', color='orange')
ax2.set_xlabel('Epoch')
ax2.set_ylabel('Accuracy (%)')
ax2.set_title('Training Accuracy over Epochs')
ax2.legend()
ax2.grid(True)

plt.tight_layout()
plt.savefig('training_progress.png')
print("Training progress saved as 'training_progress.png'")

# 10. Make predictions on sample images
print("\n7. Making predictions on sample images...")
model.eval()
with torch.no_grad():
    sample_images = X_test[:5]
    sample_labels = y_test[:5]
    predictions = model(sample_images)
    _, predicted_classes = torch.max(predictions, 1)
    
    print("Sample predictions:")
    for i in range(5):
        print(f"Image {i+1}: True label = {sample_labels[i].item()}, Predicted = {predicted_classes[i].item()}")

# 11. Save the trained model
print("\n8. Saving the trained model...")
torch.save({
    'model_state_dict': model.state_dict(),
    'optimizer_state_dict': optimizer.state_dict(),
    'train_losses': train_losses,
    'train_accuracies': train_accuracies,
}, 'image_classifier.pth')
print("Model saved as 'image_classifier.pth'")

print("\n" + "=" * 50)
print("Image Classification Example Completed!")
print("=" * 50)
print(f"Final Test Accuracy: {test_accuracy:.2f}%")
