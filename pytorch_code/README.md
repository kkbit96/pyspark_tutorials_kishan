# PyTorch Code Examples

This folder contains PyTorch examples demonstrating fundamental deep learning concepts and techniques.

## 📁 Files

### Python Scripts
- **pytorch_basics.py** - Fundamental PyTorch operations and concepts
- **pytorch_image_classification.py** - Image classification example with neural networks
- **pytorch_regression_example.py** - Linear and non-linear regression examples

### Generated Files
- **training_progress.png** - Training loss and accuracy visualization
- **regression_data.png** - Original regression data visualization
- **regression_results.png** - Model predictions vs actual data
- **regression_losses.png** - Training loss over epochs

### Saved Models
- **simple_model.pth** - Basic neural network model
- **image_classifier.pth** - Image classification model
- **linear_regression.pth** - Linear regression model
- **nonlinear_regression.pth** - Non-linear regression model

## 🚀 Running the Examples

### Prerequisites
```bash
pip install torch numpy pandas matplotlib
```

### Run Examples
```bash
# Basic PyTorch operations
python3 pytorch_basics.py

# Image classification
python3 pytorch_image_classification.py

# Regression examples
python3 pytorch_regression_example.py
```

## 📚 What Each Example Covers

### pytorch_basics.py
- Basic tensor operations (addition, multiplication, etc.)
- Matrix operations
- Gradient computation with autograd
- Simple neural network definition
- Training loop implementation
- GPU availability check
- Tensor reshaping and slicing
- Statistical operations
- NumPy-PyTorch conversion
- Model saving and loading

### pytorch_image_classification.py
- Synthetic image data generation
- DataLoader creation for batch processing
- Neural network with multiple layers and dropout
- Cross-entropy loss for classification
- Training and evaluation functions
- Training progress visualization
- Model evaluation and predictions
- Complete model checkpointing

### pytorch_regression_example.py
- Linear regression (y = 3x + 2)
- Non-linear regression (y = sin(x))
- Neural network for non-linear fitting
- Different optimizers (SGD vs Adam)
- R² score calculation
- Visualization of data and results
- Training loss plots
- Model comparison

## 🎯 Key Results

### Linear Regression
- R² Score: 0.9931 (excellent fit)
- Learned slope: 3.01 (true: 3.0)
- Learned intercept: 1.80 (true: 2.0)

### Non-linear Regression
- R² Score: 0.9624 (good fit)
- Successfully learned sin(x) pattern

## 💡 Key PyTorch Concepts Demonstrated

1. **Tensors**: PyTorch's fundamental data structure
2. **Autograd**: Automatic differentiation for gradients
3. **Neural Networks**: Building with `nn.Module`
4. **Training Loops**: Forward pass, loss computation, backpropagation
5. **Optimizers**: SGD, Adam for parameter updates
6. **Data Loading**: DataLoader for batch processing
7. **Model Persistence**: Save/load trained models
8. **GPU Support**: CUDA availability checking

## 📊 Generated Visualizations

The examples create several PNG files to visualize:
- Training progress (loss and accuracy curves)
- Original data distributions
- Model predictions vs actual values
- Training loss over epochs

## 🔧 Configuration

All examples use default configurations but can be customized:
- Learning rates
- Network architectures
- Training epochs
- Batch sizes
- Random seeds for reproducibility

## 📝 Notes

- These examples use synthetic data for demonstration
- For real-world applications, you would use actual datasets
- GPU support is checked but examples run on CPU
- Models are saved in PyTorch's `.pth` format
- Random seeds are set for reproducibility
