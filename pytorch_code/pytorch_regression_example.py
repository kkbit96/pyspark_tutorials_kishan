"""
PyTorch Regression Example
A simple linear and non-linear regression example
"""

import torch
import torch.nn as nn
import torch.optim as optim
import matplotlib.pyplot as plt
import numpy as np

print("=" * 50)
print("PyTorch Regression Example")
print("=" * 50)

# Set random seed for reproducibility
torch.manual_seed(42)
np.random.seed(42)

# 1. Generate synthetic regression data
print("\n1. Generating synthetic regression data...")

# Linear relationship: y = 3x + 2 + noise
n_samples = 100
X_linear = torch.randn(n_samples, 1) * 2  # Input features
y_linear = 3 * X_linear + 2 + torch.randn(n_samples, 1) * 0.5  # Target with noise

# Non-linear relationship: y = sin(x) + noise
X_nonlinear = torch.linspace(-3, 3, n_samples).reshape(-1, 1)
y_nonlinear = torch.sin(X_nonlinear) + torch.randn(n_samples, 1) * 0.2

print(f"Linear data: {n_samples} samples")
print(f"Non-linear data: {n_samples} samples")

# 2. Visualize the data
print("\n2. Visualizing the data...")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

ax1.scatter(X_linear.numpy(), y_linear.numpy(), alpha=0.6)
ax1.set_xlabel('X')
ax1.set_ylabel('Y')
ax1.set_title('Linear Regression Data')
ax1.grid(True)

ax2.scatter(X_nonlinear.numpy(), y_nonlinear.numpy(), alpha=0.6)
ax2.set_xlabel('X')
ax2.set_ylabel('Y')
ax2.set_title('Non-linear Regression Data')
ax2.grid(True)

plt.tight_layout()
plt.savefig('regression_data.png')
print("Data visualization saved as 'regression_data.png'")

# 3. Define Linear Regression Model
print("\n3. Defining Linear Regression Model...")

class LinearRegression(nn.Module):
    def __init__(self, input_size, output_size):
        super(LinearRegression, self).__init__()
        self.linear = nn.Linear(input_size, output_size)
    
    def forward(self, x):
        return self.linear(x)

# 4. Define Non-linear Regression Model
print("\n4. Defining Non-linear Regression Model...")

class NonLinearRegression(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(NonLinearRegression, self).__init__()
        self.network = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, output_size)
        )
    
    def forward(self, x):
        return self.network(x)

# 5. Train Linear Regression
print("\n5. Training Linear Regression...")
linear_model = LinearRegression(1, 1)
criterion = nn.MSELoss()
optimizer = optim.SGD(linear_model.parameters(), lr=0.01)

linear_losses = []
epochs = 100

for epoch in range(epochs):
    # Forward pass
    outputs = linear_model(X_linear)
    loss = criterion(outputs, y_linear)
    
    # Backward pass and optimize
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()
    
    linear_losses.append(loss.item())
    
    if (epoch + 1) % 20 == 0:
        print(f'Linear Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}')

# Get learned parameters
learned_slope = linear_model.linear.weight.item()
learned_intercept = linear_model.linear.bias.item()
print(f"True parameters: slope=3, intercept=2")
print(f"Learned parameters: slope={learned_slope:.4f}, intercept={learned_intercept:.4f}")

# 6. Train Non-linear Regression
print("\n6. Training Non-linear Regression...")
nonlinear_model = NonLinearRegression(1, 20, 1)
optimizer_nonlinear = optim.Adam(nonlinear_model.parameters(), lr=0.01)

nonlinear_losses = []
epochs = 500

for epoch in range(epochs):
    # Forward pass
    outputs = nonlinear_model(X_nonlinear)
    loss = criterion(outputs, y_nonlinear)
    
    # Backward pass and optimize
    optimizer_nonlinear.zero_grad()
    loss.backward()
    optimizer_nonlinear.step()
    
    nonlinear_losses.append(loss.item())
    
    if (epoch + 1) % 100 == 0:
        print(f'Non-linear Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}')

# 7. Visualize results
print("\n7. Visualizing regression results...")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# Linear regression results
ax1.scatter(X_linear.numpy(), y_linear.numpy(), alpha=0.6, label='Data')
with torch.no_grad():
    y_pred_linear = linear_model(X_linear)
    ax1.plot(X_linear.numpy(), y_pred_linear.numpy(), 'r-', linewidth=2, label='Prediction')
ax1.set_xlabel('X')
ax1.set_ylabel('Y')
ax1.set_title('Linear Regression Results')
ax1.legend()
ax1.grid(True)

# Non-linear regression results
ax2.scatter(X_nonlinear.numpy(), y_nonlinear.numpy(), alpha=0.6, label='Data')
with torch.no_grad():
    y_pred_nonlinear = nonlinear_model(X_nonlinear)
    ax2.plot(X_nonlinear.numpy(), y_pred_nonlinear.numpy(), 'r-', linewidth=2, label='Prediction')
ax2.set_xlabel('X')
ax2.set_ylabel('Y')
ax2.set_title('Non-linear Regression Results')
ax2.legend()
ax2.grid(True)

plt.tight_layout()
plt.savefig('regression_results.png')
print("Regression results saved as 'regression_results.png'")

# 8. Plot training losses
print("\n8. Plotting training losses...")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

ax1.plot(linear_losses)
ax1.set_xlabel('Epoch')
ax1.set_ylabel('Loss')
ax1.set_title('Linear Regression Training Loss')
ax1.grid(True)

ax2.plot(nonlinear_losses)
ax2.set_xlabel('Epoch')
ax2.set_ylabel('Loss')
ax2.set_title('Non-linear Regression Training Loss')
ax2.grid(True)

plt.tight_layout()
plt.savefig('regression_losses.png')
print("Training losses saved as 'regression_losses.png'")

# 9. Calculate R² scores
print("\n9. Calculating R² scores...")

def r_squared(y_true, y_pred):
    ss_res = ((y_true - y_pred) ** 2).sum()
    ss_tot = ((y_true - y_true.mean()) ** 2).sum()
    return 1 - (ss_res / ss_tot)

with torch.no_grad():
    r2_linear = r_squared(y_linear, linear_model(X_linear))
    r2_nonlinear = r_squared(y_nonlinear, nonlinear_model(X_nonlinear))

print(f"Linear Regression R²: {r2_linear:.4f}")
print(f"Non-linear Regression R²: {r2_nonlinear:.4f}")

# 10. Make predictions on new data
print("\n10. Making predictions on new data...")
new_X = torch.tensor([[1.5], [-1.0], [2.5]])

with torch.no_grad():
    linear_pred = linear_model(new_X)
    nonlinear_pred = nonlinear_model(new_X)

print("New data predictions:")
for i, x in enumerate(new_X):
    print(f"X = {x.item():.2f}: Linear pred = {linear_pred[i].item():.4f}, Non-linear pred = {nonlinear_pred[i].item():.4f}")

# 11. Save models
print("\n11. Saving models...")
torch.save(linear_model.state_dict(), 'linear_regression.pth')
torch.save(nonlinear_model.state_dict(), 'nonlinear_regression.pth')
print("Models saved successfully")

print("\n" + "=" * 50)
print("Regression Example Completed!")
print("=" * 50)
print(f"Linear Regression R²: {r2_linear:.4f}")
print(f"Non-linear Regression R²: {r2_nonlinear:.4f}")
