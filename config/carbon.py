# Import libraries
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn import metrics
import matplotlib.pyplot as plt
import csv

# Read the data from the csv file
#df1 = pd.read_csv('AWS EC2 Carbon Footprint Dataset - EC2 Instances Dataset.csv')
# Show first five rows
#print(df1.head())


def estimate_coef(x, y):
	# number of observations/points
	n = np.size(x)

	# mean of x and y vector
	m_x = np.mean(x)
	m_y = np.mean(y)

	# calculating cross-deviation and deviation about x
	SS_xy = np.sum(y*x) - n*m_y*m_x
	SS_xx = np.sum(x*x) - n*m_x*m_x

	# calculating regression coefficients
	b_1 = SS_xy / SS_xx
	b_0 = m_y - b_1*m_x

	return (b_0, b_1)

def plot_regression_line(x, y, b):
	# plotting the actual points as scatter plot
	plt.scatter(x, y, color = "m",
			marker = "o", s = 30)

	# predicted response vector
	y_pred = b[0] + b[1]*x

	# plotting the regression line
	plt.plot(x, y_pred, color = "g")

	# putting labels
	plt.xlabel('x')
	plt.ylabel('y')

	# function to show plot
	plt.show()

def main():
	# observations / data	
	with open("AWS EC2 Carbon Footprint Dataset - EC2 Instances Dataset.csv", 'r') as file:
		csvreader = csv.reader(file)
		i = 0
		for row in csvreader:
			if i == 0:
				i = 1
				pass		
			else:
				y = np.array([float(row[27].replace(',', '.')), float(row[28].replace(',', '.')), float(row[29].replace(',', '.')), float(row[30].replace(',', '.'))])
				print(y)
	x = np.array([0, 10, 50, 100])

	# estimating coefficients
	b = estimate_coef(x, y)
	print("Estimated coefficients:\nb_0 = {} \
		\nb_1 = {}".format(b[0], b[1]))

	# plotting regression line
	plot_regression_line(x, y, b)

if __name__ == "__main__":
	main()