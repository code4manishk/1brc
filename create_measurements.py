import sys
import random
import os

def create_measurement(input_path, output_path, total):
    if os.path.exists(output_path):
        print("{} already exists".format(output_path))
        return

    names = list()
    with open(input_path) as f:
        for name in f:
            names.append(name.strip())

    num_cities = len(names)
    with open(output_path, "w") as f:
        for m in range(0, total):
            idx = random.randint(0, num_cities-1)
            f.write("{};{:.1f}\n".format(names[idx], random.uniform(-75.0, +55.0)))

def main(argv):
    cities_file_path = argv[1]
    output_path = argv[2]
    num_entries = int(argv[3])

    create_measurement(cities_file_path, output_path, num_entries)
    return 0

if __name__ == "__main__":
    main(sys.argv)
