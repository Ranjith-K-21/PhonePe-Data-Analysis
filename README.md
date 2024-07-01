Here's the updated README file with sections added for maps and top analysis:

---

# PhonePe Data Analysis Application

## Table of Contents
1. [Overview](#overview)
2. [Features](#features)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Customization](#customization)
6. [Code Structure](#code-structure)
7. [Detailed Functionality](#detailed-functionality)
8. [Contributing](#contributing)
9. [License](#license)
10. [Acknowledgements](#acknowledgements)
11. [Contact](#contact)

## Overview

This project is designed to provide an interactive analysis tool for PhonePe transaction data using Streamlit. The application allows users to visualize and analyze transaction trends over time, offering insights into various parameters such as transaction value, transaction count, and user behavior. The tool leverages SQL queries to fetch data, processes it using Pandas, and presents the results through interactive charts and maps created with Plotly.

## Features

- **Interactive Data Visualization**: 
  - Line charts showing transaction trends over quarters and years.
  - Choropleth maps for geographical analysis of transactions.
  
- **User-Friendly Interface**:
  - Dropdown menus for selecting analysis parameters.
  - Customizable charts with hover labels and formatted data.

- **Data Processing**:
  - Grouping and summarizing transaction data by year and quarter.
  - Formatting numbers into the Indian numbering system.

- **Customizable UI Elements**:
  - Background colors, text colors, and grid styles for charts.
  - Adjustable margins and padding for container elements.

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/Ranjith-K-21//phonepe-data-analysis.git
    cd phonepe-data-analysis
    ```

2. Install the required packages:
    ```bash
    pip install -r requirements.txt
    ```

3. Run the Streamlit application:
    ```bash
    streamlit run PhonePe_Data_Analysis.py
    ```

## Usage

1. **Select Analysis Parameter**:
   - Choose between `Transaction`, `Insurance`, or `User` from the dropdown menu.
   - Based on the selected parameter, choose a sub-parameter for detailed analysis (e.g., Transaction value, Transaction count).

2. **Visualize Data**:
   - The application will display line charts for the selected parameters, with options to view data by year and quarter.
   - Hover over the data points to see detailed information in a formatted manner.

3. **Navigate**:
   - Use the sidebar to navigate between different sections of the app:
     - **Home**: Introduction and instructions on using the app.
     - **Data Upload**: Upload data from various sources.
     - **Data Analytics**: Perform different types of data analysis.

## Customization

### Chart Appearance

- **Colors**: The chart colors can be customized in the `result_line_chart()` function.
- **Hover Labels**: Customize the hover labels' background color, border color, and font color.
- **Grid Lines**: Adjust the visibility and color of grid lines on the charts.

### Locale and Number Formatting

- The application uses the Indian numbering system for formatting large numbers. This can be adjusted in the `format_indian_number` function.

## Code Structure

- **PhonePe_Data_Analysis.py**: Main Streamlit application file containing all the logic for data processing and visualization.
  - **Import Libraries**: Essential libraries such as `pandas`, `numpy`, `plotly.graph_objects`, and `streamlit`.
  - **SQL Query**: Fetches data from the database using SQLAlchemy.
  - **Data Processing**: Handles grouping and summarizing transaction data.
  - **Locale Settings**: Configures the locale for number formatting.
  - **Number Formatting Function**: Formats numbers into the Indian numbering system with appropriate suffixes.
  - **Interactive Widgets**: Dropdown menus for selecting analysis parameters and sub-parameters.
  - **Chart Functions**: Functions to create and customize line charts and maps.
  - **Styling**: Custom CSS to adjust the appearance of Streamlit components.

## Detailed Functionality

### Data Fetching and Processing

- The application fetches data from a SQL database containing transaction details.
- Data is grouped by year and quarter, and aggregated based on the selected parameter (e.g., transaction value, transaction count).

### Visualization

- Line charts are created using Plotly to visualize the trends over time.
- Choropleth maps are used for geographical analysis, displaying transaction data on a state-by-state basis.

### User Interface

- The application features dropdown menus for parameter selection, which dynamically update the charts based on user input.
- Custom CSS is used to ensure a cohesive and visually appealing user interface.

### Maps Analysis

- The application includes interactive choropleth maps for geographical analysis of transactions.
- These maps allow users to visualize transaction data on a state-by-state basis, providing insights into regional trends and patterns.

### Top Analysis

- The application provides top analysis features to identify the top-performing states, users, or transaction types.
- This helps in recognizing key areas of high activity or value, aiding in strategic decision-making.

## Contributing

We welcome contributions to enhance the functionality and features of this application. Please follow these steps to contribute:
1. Fork the repository.
2. Create a new branch:
    ```bash
    git checkout -b feature-branch
    ```
3. Make your changes and commit them:
    ```bash
    git commit -m 'Add new feature'
    ```
4. Push to the branch:
    ```bash
    git push origin feature-branch
    ```
5. Create a pull request.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.

## Acknowledgements

- The project uses data from PhonePe transactions.
- Special thanks to PhonePe for providing the data on their [GitHub repository](https://github.com/PhonePe/pulse).
- Special thanks to the Streamlit community for their support and resources.
