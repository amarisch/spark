from pyspark import SparkConf, SparkContext


def loadMovieNames():
	movienames = {}
	with open("ml-100k/u.item") as f:
		for line in f:
			fields = line.split('|')
			movienames[int(fields[0])] = fields[1]
	return movienames

def parseInput(line):
	fields = line.split()
	return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == '__main__':
	# the main script creating our SparkContext
	conf = SparkConf().setAppName("WorstMovies")
	sc = SparkContext(conf = conf)

	# Load up our movie ID -> movie name lookup table
	movienames = loadMovieNames()

	# load raw u.data file
	lines = sc.textFile('ml-100k/u.data')

	# COnvert to (movieID, (rating, 1.0))
	movieratings = lines.map(parseInput)

	# Reduce to (movieID, (sumOfrating, totalRatings))
	ratingTotalsAndCount = movieratings.reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[1]))

	# Map to (movieID, averageRating)
	averageRatings = ratingTotalsAndCount.mapValue(lambda totalAndCount: totalAndCount[0] / totalAndCount[1])

	# Sort by average rating
	sortedmovies = averageRatings.sortBy(lambda x: x[1])

	# Take the top 10 results
	results = sortedmovies.take(10)