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
    file_path = './long'  # Directory to store trace files
    num_keys = 3
    get_ratio = 9
    put_ratio = 9
    delete_ratio = 0
    num_operations_per_file = 10000
    
    generate_trace_files([file_path + str(i + 2) + ".txt" for i in range(num_files)], num_keys, [get_ratio, put_ratio, delete_ratio], num_operations_per_file)
