import math
from collections import deque


def haversine(lat1, lon1, lat2, lon2, degrees=False):
    R = 6371  # Radius of Earth in kilometers
    if degrees:
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance * 1000  # Convert to meters if needed


def bearing(lat1, lon1, lat2, lon2, degrees=False):
    if degrees:
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1

    x = math.cos(lat2) * math.sin(dlon)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    bearing = (math.atan2(x, y) + 2 * math.pi) % (2 * math.pi)  # Normalize to [0, 2π)
    bearing_deg = (math.degrees(bearing) + 360) % 360
    return bearing if not degrees else bearing_deg


def normalize_angle_0_2pi(angle: float) -> float:
    """
    Normalize an angle in radians to the range [0, 2*pi).

    :param angle: The angle in radians.
    :return: The normalized angle in the range [0, 2*pi).
    """
    return angle % (2 * math.pi)


def normalize_angle_mpi_pi(angle: float) -> float:
    """
    Normalize an angle in radians to the range (-pi, pi].

    This is often used for calculating the shortest angle difference.

    :param angle: The angle in radians.
    :return: The normalized angle in the range (-pi, pi].
    """
    return math.atan2(math.sin(angle), math.cos(angle))


class CircularMean:
    def __init__(self, degrees=False):
        """
        Initializes the CircularMean with an option to handle degrees or radians.

        :param degrees: If True, the input angles are in degrees, otherwise in radians.
        """
        self.degrees = degrees
        self.clear()

    def clear(self):
        """Reset the accumulators and sample count."""
        self._sin_accum = 0.0
        self._cos_accum = 0.0
        self._sample_size = 0
    
    def merge_with(self, other):
        """
        Merge the statistics from another CircularMean object into this one.

        :param other: The other CircularMean object to merge.
        """
        if self.degrees != other.degrees:
            raise ValueError("Cannot merge CircularMean objects with different angle units (degrees vs. radians).")
        self._sin_accum += other._sin_accum
        self._cos_accum += other._cos_accum
        self._sample_size += other._sample_size
        return self

    def update(self, value):
        """
        Update with a new angle value. Converts to radians if degrees=True.
        Returns the updated circular mean.

        :param value: The angle value (either in degrees or radians).
        """
        if self.degrees:
            value = math.radians(value)  # Convert degrees to radians
        value = self._normalize_radian(value)
        self._sin_accum += math.sin(value)
        self._cos_accum += math.cos(value)
        self._sample_size += 1
        return self.mean()

    def mean(self):
        """Return the current circular mean (in radians or degrees depending on flag)."""
        if self.sample_size():
            avg_sin = self._sin_accum / self.sample_size()
            avg_cos = self._cos_accum / self.sample_size()
            mean_rad = math.atan2(avg_sin, avg_cos)
            return math.degrees(mean_rad) if self.degrees else mean_rad
        return 0.0

    def std_dev(self):
        """
        Return the circular standard deviation.
        This is based on the radius (mean resultant length).
        """
        radius = self.radius()
        if radius == 0:
            return float('inf')  # Infinite SD when all angles are uniform
        return math.sqrt(-2 * math.log(radius))

    def radius(self):
        """
        Return the mean resultant length (vector radius).
        1.0 if all angles are identical, 0.0 if angles are uniformly distributed.
        """
        if self.sample_size():
            avg_sin = self._sin_accum / self.sample_size()
            avg_cos = self._cos_accum / self.sample_size()
            radius = math.sqrt(avg_sin**2 + avg_cos**2)
            return radius
        return 0.0

    def sample_size(self):
        """Return the number of samples added."""
        return self._sample_size

    @staticmethod
    def _normalize_radian(angle):
        """Normalize angle to [0, 2π)."""
        return angle % (2 * math.pi)

class MeanStats:
    def __init__(self):
        """Initialize the statistics tracker."""
        self.clear()

    def clear(self):
        """Reset the accumulators and sample count."""
        self._values = []
        self._sample_size = 0

    def merge_with(self, other):
        """
        Merge the statistics from another MeanStats object into this one.

        :param other: The other MeanStats object to merge.
        """
        self._values.extend(other._values)
        self._sample_size += other._sample_size
        return self

    def update(self, value):
        """
        Update with a new data point and return the updated mean.

        :param value: The new data point.
        """
        self._values.append(value)
        self._sample_size += 1
        return self.mean()

    def mean(self):
        """Return the current arithmetic mean."""
        if self.sample_size() > 0:
            return sum(self._values) / self.sample_size()
        return 0.0

    def std_dev(self):
        """
        Return the standard deviation of the sample.
        If there's less than 2 values, the standard deviation is 0.
        """
        if self.sample_size() < 2:
            return 0.0
        mean_value = self.mean()
        squared_diffs = [(x - mean_value) ** 2 for x in self._values]
        return math.sqrt(sum(squared_diffs) / self.sample_size())

    def sample_size(self):
        """Return the number of data points in the sample."""
        return self._sample_size


class SlidingWindow:
    def __init__(self, window_size: int = 10):
        """
        Initialize the sliding window filter.
        :param window_size: The size of the sliding window.
        """
        if window_size <= 0:
            raise ValueError("Window size must be a positive integer.")
        self.window_size = window_size
        self._values = deque(maxlen=window_size)
        self._sum = 0.0
        self._sum_sq = 0.0

    def clear(self):
        """Reset the filter."""
        self._values.clear()
        self._sum = 0.0
        self._sum_sq = 0.0

    def update(self, value: float):
        """
        Update the window with a new value.
        :param value: The new data point.
        """
        if len(self._values) == self.window_size:
            # The oldest value is about to be evicted
            old_value = self._values[0]
            self._sum -= old_value
            self._sum_sq -= old_value**2

        self._values.append(value)
        self._sum += value
        self._sum_sq += value**2

    def mean(self) -> float:
        """Return the current mean of the values in the window."""
        if not self._values:
            return 0.0
        return self._sum / len(self._values)

    def std_dev(self) -> float:
        """
        Return the standard deviation of the values in the window.
        """
        n = len(self._values)
        if n < 2:
            return 0.0
        
        mean = self.mean()
        # Variance = E[X^2] - (E[X])^2
        variance = (self._sum_sq / n) - (mean**2)
        
        # The variance can be slightly negative due to floating point inaccuracies
        if variance < 0:
            return 0.0
            
        return math.sqrt(variance)

    def sample_size(self) -> int:
        """Return the current number of values in the window."""
        return len(self._values)

    def is_full(self) -> bool:
        """Check if the window is full."""
        return len(self._values) == self.window_size
