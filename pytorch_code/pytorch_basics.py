"""
Basic PyTorch Examples
This file demonstrates fundamental PyTorch operations and concepts
"""

import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np

print("=" * 50)
print("PyTorch Basic Examples")
print("=" * 50)

# 1. Basic Tensor Operations
print("\n1. BASIC TENSOR OPERATIONS")
print("-" * 30)

# Creating tensors
print("Creating tensors:")
x = torch.tensor([1, 2, 3, 4])
y = torch.tensor([5, 6, 7, 8])
print(f"Tensor x: {x}")
print(f"Tensor y: {y}")

# Basic operations
print(f"\nAddition: {x + y}")
print(f"Subtraction: {x - y}")
print(f"Multiplication: {x * y}")
print(f"Division: {x / y}")

# Matrix operations
print("\nMatrix operations:")
matrix_a = torch.tensor([[1, 2], [3, 4]])
matrix_b = torch.tensor([[5, 6], [7, 8]])
print(f"Matrix A:\n{matrix_a}")
print(f"Matrix B:\n{matrix_b}")
print(f"Matrix multiplication:\n{torch.matmul(matrix_a, matrix_b)}")

# 2. Tensor Operations with Gradients
print("\n2. TENSOR OPERATIONS WITH GRADIENTS")
print("-" * 40)

# Create tensor with gradient tracking
x_grad = torch.tensor([2.0, 3.0], requires_grad=True)
y_grad = torch.tensor([4.0, 5.0], requires_grad=True)

# Perform operations
z = x_grad * y_grad
sum_z = z.sum()

print(f"x: {x_grad}, requires_grad: {x_grad.requires_grad}")
print(f"y: {y_grad}, requires_grad: {y_grad.requires_grad}")
print(f"z = x * y: {z}")
print(f"sum(z): {sum_z}")

# Compute gradients
sum_z.backward()
print(f"Gradient of x: {x_grad.grad}")
print(f"Gradient of y: {y_grad.grad}")

# 3. Simple Neural Network
print("\n3. SIMPLE NEURAL NETWORK")
print("-" * 30)

# Define a simple neural network
class SimpleNN(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(SimpleNN, self).__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_size, output_size)
    
    def forward(self, x):
        x = self.fc1(x)
        x = self.relu(x)
        x = self.fc2(x)
        return x

# Create the network
input_size = 2
hidden_size = 4
output_size = 1
model = SimpleNN(input_size, hidden_size, output_size)

print(f"Network architecture:\n{model}")

# 4. Training Example
print("\n4. TRAINING EXAMPLE")
print("-" * 25)

# Generate synthetic data
torch.manual_seed(42)
X_train = torch.randn(100, 2)
y_train = torch.randn(100, 1)

# Loss function and optimizer
criterion = nn.MSELoss()
optimizer = optim.SGD(model.parameters(), lr=0.01)

# Training loop
epochs = 100
losses = []

print("Training started...")
for epoch in range(epochs):
    # Forward pass
    outputs = model(X_train)
    loss = criterion(outputs, y_train)
    
    # Backward pass and optimization
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()
    
    losses.append(loss.item())
    
    if (epoch + 1) % 20 == 0:
        print(f'Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}')

print(f"Final loss: {losses[-1]:.4f}")

# 5. GPU Availability Check
print("\n5. GPU AVAILABILITY")
print("-" * 20)
print(f"CUDA available: {torch.cuda.is_available()}")
if torch.cuda.is_available():
    print(f"CUDA device: {torch.cuda.get_device_name(0)}")
    print(f"Number of GPUs: {torch.cuda.device_count()}")

# 6. Common Tensor Operations
print("\n6. COMMON TENSOR OPERATIONS")
print("-" * 30)

# Reshaping
tensor = torch.arange(12)
print(f"Original tensor: {tensor}")
print(f"Reshaped to 3x4: {tensor.reshape(3, 4)}")

# Slicing
matrix = torch.tensor([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
print(f"\nOriginal matrix:\n{matrix}")
print(f"First row: {matrix[0, :]}")
print(f"First column: {matrix[:, 0]}")
print(f"Submatrix [1:3, 1:3]:\n{matrix[1:3, 1:3]}")

# Statistical operations
data = torch.tensor([1.0, 2.0, 3.0, 4.0, 5.0])
print(f"\nData: {data}")
print(f"Mean: {data.mean()}")
print(f"Std: {data.std()}")
print(f"Max: {data.max()}, Index: {data.argmax()}")
print(f"Min: {data.min()}, Index: {data.argmin()}")

# 7. Conversion between NumPy and PyTorch
print("\n7. NUMPY - PYTORCH CONVERSION")
print("-" * 35)

numpy_array = np.array([1, 2, 3, 4, 5])
torch_tensor = torch.from_numpy(numpy_array)
print(f"NumPy array: {numpy_array}")
print(f"PyTorch tensor: {torch_tensor}")

back_to_numpy = torch_tensor.numpy()
print(f"Back to NumPy: {back_to_numpy}")

# 8. Saving and Loading Models
print("\n8. SAVING AND LOADING MODELS")
print("-" * 30)

# Save model
torch.save(model.state_dict(), 'simple_model.pth')
print("Model saved as 'simple_model.pth'")

# Load model
loaded_model = SimpleNN(input_size, hidden_size, output_size)
loaded_model.load_state_dict(torch.load('simple_model.pth'))
loaded_model.eval()
print("Model loaded successfully")

# Test loaded model
test_input = torch.randn(1, 2)
with torch.no_grad():
    prediction = loaded_model(test_input)
print(f"Prediction from loaded model: {prediction}")

print("\n" + "=" * 50)
print("PyTorch Examples Completed!")
print("=" * 50)
