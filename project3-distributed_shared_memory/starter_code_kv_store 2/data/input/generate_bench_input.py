import random
import os

def generate_trace_files(file_paths, num_keys, get_put_delete_ratio, num_operations_per_file):

    # Generate the common set of keys
    key_set = [f"key_{i}" for i in range(1, num_keys + 1)]

    # Initialize the value counter for 'put' operations
    put_value_counter = 1

    # Generate the operations based on the given ratios
    total_ratio = sum(get_put_delete_ratio)
    operations = ['get'] * get_put_delete_ratio[0] + ['put'] * get_put_delete_ratio[1] + ['delete'] * get_put_delete_ratio[2]
    
    # Generate trace files
    for file_path in file_paths:
        with open(file_path, "w") as f:
            for _ in range(num_operations_per_file):
                operation = random.choice(operations)
                key = random.choice(key_set)
                
                # If operation is 'put', include a value
                if operation == 'put':
                    f.write(f"put {key} {put_value_counter}\n")
                    put_value_counter += 1
                else:
                    f.write(f"{operation} {key}\n")

if __name__ == '__main__':
    num_files = 8
    file_paths = ['./read_mostly_low_contention', './read_mostly_high_contention', './write_mostly_low_contention', './write_mostly_high_contention']  # Directory to store trace files
    num_keyss = [10000, 10, 10000, 10]
    get_ratios = [19, 19, 1, 1]
    put_ratios = [1, 1, 19, 19]
    delete_ratios = [0, 0, 0, 0]
    num_operations_per_file = 10000
    
    for file_path, num_keys, get_ratio, put_ratio, delete_ratio in zip(file_paths, num_keyss, get_ratios, put_ratios, delete_ratios):
        generate_trace_files([file_path + str(i + 2) + ".txt" for i in range(num_files)], num_keys, [get_ratio, put_ratio, delete_ratio], num_operations_per_file)
