#!/usr/bin/env zsh

# NOTE: This script takes too long to run during Claude sessions
# It must be run manually by the user when needed

# Exit on any error, including from subshells
set -e
set -o pipefail

# Add debug trap
trap 'echo "ERROR in $0 at line $LINENO: Command failed with exit code $?" >&2' ERR

# Configuration
PROJECT_ROOT="."
OUTPUT_FILE=""
SINGLE_FILE=""
OUTPUT_TO_STDOUT=0
STDIN_MODE=0

# Global associative array for file to crate mapping
typeset -A FILE_TO_CRATE_MAP

# Global variable to store file contents
FILE_CONTENTS=""

# Function to read file contents into global FILE_CONTENTS variable
read_file_to_global() {
    local file=$1
    if [[ -f "$file" ]]; then
        FILE_CONTENTS=$(<"$file")

        if [[ -n "$DEBUG_LINE_NUMBERS" ]]; then
            echo "DEBUG: read_file_to_global: $file" >&2
            echo "DEBUG:   From variable bytes: ${#FILE_CONTENTS}" >&2
            echo "DEBUG:   Filesystem bytes: $(wc -c < "$file")" >&2
            echo "DEBUG:   First 50 chars from variable: '${FILE_CONTENTS:0:50}'" >&2
            echo "DEBUG:   First 50 chars from filesystem: '$(head -c 50 "$file")'" >&2
        fi
    else
        echo "Error: File $file does not exist" >&2
        return 1
    fi
}

# Function to read stdin contents into global FILE_CONTENTS variable
read_stdin_to_global() {
    FILE_CONTENTS=$(cat)

    if [[ -n "$DEBUG_LINE_NUMBERS" ]]; then
        echo "DEBUG: read_stdin_to_global" >&2
        echo "DEBUG:   From variable bytes: ${#FILE_CONTENTS}" >&2
        echo "DEBUG:   First 50 chars from variable: '${FILE_CONTENTS:0:50}'" >&2
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --project-root)
            PROJECT_ROOT="$2"
            shift 2
            ;;
        --file)
            SINGLE_FILE="$2"
            OUTPUT_TO_STDOUT=1
            shift 2
            ;;
        --stdin)
            STDIN_MODE=1
            shift 1
            ;;
        *)
            echo "Usage: $0 [--project-root <path>] [--file <specific-file.rs>] [--stdin]" >&2
            echo "  --file: analyze a specific file and output to stdout" >&2
            echo "  --stdin: read file contents from stdin (requires --file to specify path)" >&2
            exit 1
            ;;
    esac
done

# Validate that --stdin requires --file
if [[ $STDIN_MODE -eq 1 ]] && [[ -z "$SINGLE_FILE" ]]; then
    echo "Error: --stdin requires --file to specify the file path" >&2
    echo "Example: $0 --file src/main.rs --stdin < actual_content.rs" >&2
    exit 1
fi

# Read stdin once if in stdin mode
if [[ $STDIN_MODE -eq 1 ]]; then
    read_stdin_to_global
fi


# Canonicalize PROJECT_ROOT to handle symlinks consistently throughout the script
# Also remove any trailing slash for consistent path manipulation
PROJECT_ROOT="${PROJECT_ROOT:A}"
PROJECT_ROOT="${PROJECT_ROOT%/}"

# Set output file to stdout or relative to project root
if [[ $OUTPUT_TO_STDOUT -eq 1 ]]; then
    OUTPUT_FILE="/dev/stdout"
else
    OUTPUT_FILE="$PROJECT_ROOT/.agentic-shepherd/code-map.md"
fi

# Function to discover all Rust files in workspace
discover_workspace_rust_files() {
    local -a rust_files

    # Save current directory and change to project root
    local original_dir=$(pwd)
    cd "$PROJECT_ROOT" || return 1

    # Use cargo metadata to find all workspace members
    local metadata=$(cargo metadata --no-deps --format-version=1 2>/dev/null)

    if [[ -n "$metadata" ]]; then
        # Extract all package directories from metadata
        local package_dirs=$(echo "$metadata" | jq -r '.packages[].manifest_path' | xargs -n1 dirname)

        # Find all .rs files in each package's src directory
        for pkg_dir in ${(f)package_dirs}; do
            # Make path relative using our function which handles realpath
            local rel_pkg_dir=$(make_relative_path "$pkg_dir")

            # Check if src directory exists
            if [[ -d "$rel_pkg_dir/src" ]]; then
                # Add files using relative path
                local found_files=($rel_pkg_dir/src/**/*.rs(N))
                # Strip ./ prefix from each file path
                for f in "${found_files[@]}"; do
                    rust_files+=("${f#./}")
                done
            else
            fi
        done
    else
        echo "ERROR: cargo metadata returned nothing"
        exit 1
    fi

    # Return to original directory
    cd "$original_dir"

    echo "${rust_files[@]}"
}

# Build a mapping from file paths to crate names using cargo metadata
# Populates the global FILE_TO_CRATE_MAP associative array
build_file_to_crate_mapping() {
    # Save current directory and change to project root
    local original_dir=$(pwd)
    cd "$PROJECT_ROOT" || return 1

    # Get cargo metadata
    local metadata=$(cargo metadata --no-deps --format-version=1 2>/dev/null)

    if [[ -n "$metadata" ]]; then
        # Process each package
        local packages=$(echo "$metadata" | jq -c '.packages[]')

        while IFS= read -r package; do
            local crate_name=$(echo "$package" | jq -r '.name' | tr '-' '_')
            local manifest_path=$(echo "$package" | jq -r '.manifest_path')
            local pkg_dir=$(dirname "$manifest_path")

            # Make path relative
            local rel_pkg_dir=$(make_relative_path "$pkg_dir")

            # Map all .rs files in this package to the crate name
            if [[ -d "$rel_pkg_dir/src" ]]; then
                local found_files=($rel_pkg_dir/src/**/*.rs(N))
                for f in "${found_files[@]}"; do
                    local clean_path="${f#./}"
                    FILE_TO_CRATE_MAP[$clean_path]=$crate_name
                done
            fi
        done <<< "$packages"
    fi

    # Return to original directory
    cd "$original_dir"
}


# Function to build byte-to-line mapping from stdin
build_line_map_from_stdin() {
    local -a line_starts
    local byte_count=0
    local line_num=0
    local last_line=""

    line_starts+=(0)

    # Add debug to see if we're getting any input
    if [[ -n "$DEBUG_LINE_NUMBERS" ]]; then
        echo "DEBUG: build_line_map_from_stdin starting..." >&2
    fi

    while IFS= read -r line; do
        # Add length of line plus newline
        byte_count=$((byte_count + ${#line} + 1))
        line_starts+=($byte_count)
        line_num=$((line_num + 1))
        last_line="$line"

        if [[ -n "$DEBUG_LINE_NUMBERS" ]] && [[ $line_num -le 5 ]]; then
            echo "DEBUG: Line $line_num: length=${#line}, cumulative_bytes=$byte_count" >&2
        fi
    done

    if [[ -n "$DEBUG_LINE_NUMBERS" ]]; then
        echo "DEBUG: Total lines read: $line_num" >&2
        echo "DEBUG: Total bytes: $byte_count" >&2
        echo "DEBUG: Last line was: '${last_line:0:50}...'" >&2
        echo "DEBUG: line_starts array size: ${#line_starts[@]}" >&2
    fi

    echo "${line_starts[@]}"
}

# Function to convert byte offset to line number
get_line_number() {
    local offset=$1
    shift
    local -a line_starts=("$@")
    local line_num=1

    # Debug: Show what we're working with
    if [[ -n "$DEBUG_LINE_NUMBERS" ]]; then
        echo "DEBUG: get_line_number called with offset=$offset" >&2
        echo "DEBUG: line_starts array has ${#line_starts[@]} elements" >&2
        echo "DEBUG: First few line_starts: ${line_starts[@]:0:5}" >&2
    fi

    for start in "${line_starts[@]}"; do
        if [[ $offset -lt $start ]]; then
            break
        fi
        ((line_num+=1))
    done

    local result=$((line_num - 1))

    if [[ -n "$DEBUG_LINE_NUMBERS" ]]; then
        echo "DEBUG: Offset $offset -> line $result" >&2
    fi

    echo $result
}

# Function to check that a path is relative (not absolute)
check_relative_path() {
    local path=$1

    if [[ "$path" = /* ]]; then
        echo "ERROR: Expected relative path but got absolute path: $path" >&2
        exit 1
    fi
}

# Format a file header with crate information
format_file_header() {
    local file=$1
    local crate_name="${FILE_TO_CRATE_MAP[$file]}"

    if [[ -z "$crate_name" ]]; then
        echo "ERROR: No crate mapping found for file: $file" >&2
        echo "ERROR: This should not happen - all files in code map should belong to a crate" >&2
        exit 1
    fi

    echo "### $file [crate: $crate_name]"
}

# Function to ensure a path is relative to PROJECT_ROOT
make_relative_path() {
    local path=$1

    # Resolve any symlinks in the input path using zsh's :A modifier
    path=${path:A}

    if [[ "$path" = /* ]]; then
        # If path is absolute, make it relative to PROJECT_ROOT
        if [[ "$path" = "$PROJECT_ROOT"/* ]]; then
            # Path starts with project root - strip it and the following slash
            echo "${path#$PROJECT_ROOT/}"
        elif [[ "$path" = "$PROJECT_ROOT" ]]; then
            # Path is exactly the project root
            echo "."
        else
            # Error: absolute path not under project root
            echo "ERROR: Path '$path' is not under PROJECT_ROOT '$PROJECT_ROOT'" >&2
            exit 1
        fi
    else
        echo "$path"
    fi
}

# Process rust files and extract specific symbol kinds
process_symbols() {
    local section_title=$1
    local symbol_kind=$2
    local pass_number=$3

    echo "Processing: $section_title..." >&2

    echo "## $section_title" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"

    # Use same file list as traits processing
    local -a files_to_process
    if [[ -n "$SINGLE_FILE" ]]; then
        files_to_process=("$SINGLE_FILE")
    else
        files_to_process=($(discover_workspace_rust_files))
    fi

    local file_count=0
    local total_files=${#files_to_process[@]}
    local total_passes=5
    local total_file_scans=$((total_files * total_passes))

    for file in "${files_to_process[@]}"; do
        ((file_count+=1))
        local overall_progress=$(((pass_number - 1) * total_files + file_count))
        echo "  [$overall_progress/$total_file_scans] Analyzing $file..." >&2

        # Read file contents (stdin already read at startup)
        if [[ $STDIN_MODE -ne 1 ]]; then
            read_file_to_global "$file"
        fi

        local -a line_starts
        local symbols="init string"

        # Always use FILE_CONTENTS
        if [[ -n "$DEBUG_LINE_NUMBERS" ]]; then
            echo "DEBUG: Processing file: $file" >&2
            echo "DEBUG: FILE_CONTENTS length: ${#FILE_CONTENTS}" >&2
            echo "DEBUG: FILE_CONTENTS bytes via printf: $(printf '%s' "$FILE_CONTENTS" | wc -c)" >&2
            echo "DEBUG: FILE_CONTENTS bytes via echo: $(echo "$FILE_CONTENTS" | wc -c)" >&2
        fi

        # Use printf instead of echo to avoid adding extra newline
        if [[ -n "$DEBUG_LINE_NUMBERS" ]]; then
            echo "DEBUG: About to build line map..." >&2
            echo "DEBUG: FILE_CONTENTS exists: $([[ -n "$FILE_CONTENTS" ]] && echo "yes" || echo "no")" >&2
        fi

        line_starts=($(printf '%s' "$FILE_CONTENTS" | build_line_map_from_stdin))

        if [[ -n "$DEBUG_LINE_NUMBERS" ]]; then
            echo "DEBUG: line_starts result: ${line_starts[@]:0:5}..." >&2
        fi

        symbols=$(printf '%s' "$FILE_CONTENTS" | rust-analyzer symbols 2>/dev/null)
        local found_items=0

        # For all symbol types, use structured parsing to handle parent relationships
        # Parse symbols into arrays
        local -a node_parents node_labels node_kinds node_ranges node_details
        parse_rust_analyzer_output "$symbols"

        if [[ "$symbol_kind" == "Impl" ]]; then
            # Find impl blocks
            local i=1
            while [[ $i -le ${#node_kinds[@]} ]]; do
                if [[ "${node_kinds[$i]}" == "Impl" ]]; then
                    if [[ $found_items -eq 0 ]]; then
                        check_relative_path "$file"
                        format_file_header "$file" >> "$OUTPUT_FILE"
                        found_items=1
                    fi

                    local impl_label="${node_labels[$i]}"
                    local impl_line_num=$(get_line_number "${node_ranges[$i]}" "${line_starts[@]}")
                    local impl_module_path=$(build_module_path $i "$file")
                    echo "- $impl_label: $impl_line_num [module: $impl_module_path]" >> "$OUTPUT_FILE"

                    # Extract the impl context for methods
                    local method_impl_context=$(extract_impl_context "$impl_label" "method")

                    # Find methods in this impl block
                    local rust_analyzer_idx=0
                    local k=1
                    while [[ $k -lt $i ]]; do
                        if [[ -n "${node_labels[$k]}" ]]; then
                            ((rust_analyzer_idx+=1))
                        fi
                        ((k+=1))
                    done

                    # Look for methods that have this impl as parent
                    local j=1
                    while [[ $j -le ${#node_parents[@]} ]]; do
                        if [[ "${node_parents[$j]}" == "$rust_analyzer_idx" ]] && [[ "${node_kinds[$j]}" =~ ^(Function|Method)$ ]]; then
                            local method_line_num=$(get_line_number "${node_ranges[$j]}" "${line_starts[@]}")
                            # Build module path with impl context for methods
                            local method_base_path="nothing"
                            if [[ "$file" =~ (^|/)src/(lib|main)\.rs$ ]]; then
                                method_base_path="crate"
                            elif [[ "$file" =~ (^|/)src/(.+)\.rs$ ]]; then
                                local module_from_file="${match[2]//\//:}"
                                method_base_path="crate::${module_from_file}"
                            fi

                            # Construct final module path
                            local method_module_path="${method_base_path}"
                            if [[ -n "$method_impl_context" ]]; then
                                method_module_path="${method_base_path}::${method_impl_context}"
                            fi
                            if [[ -n "${node_details[$j]}" ]]; then
                                echo "  - fn ${node_labels[$j]}: $method_line_num (${node_details[$j]}) [module: $method_module_path]" >> "$OUTPUT_FILE"
                            else
                                echo "  - fn ${node_labels[$j]}: $method_line_num [module: $method_module_path]" >> "$OUTPUT_FILE"
                            fi
                        fi
                        ((j+=1))
                    done
                fi
                ((i+=1))
            done
        elif [[ "$symbol_kind" == "Function" ]]; then
            # Include all functions (including those in test modules)
            local i=1
            while [[ $i -le ${#node_kinds[@]} ]]; do
                if [[ "${node_kinds[$i]}" == "Function" ]]; then
                    # Skip functions that are inside impl blocks or trait blocks (methods)
                    # Check if parent is an Impl or Trait block
                    local parent_idx="${node_parents[$i]}"
                    local skip=0
                    if [[ "$parent_idx" != "-1" ]] && [[ "$parent_idx" =~ ^[0-9]+$ ]]; then
                        # Convert rust-analyzer node ID to our array index
                        # The parent_idx is the 0-based position in rust-analyzer's output
                        # We need to find which array index corresponds to that rust-analyzer node ID
                        local rust_analyzer_idx=0
                        local k=1

                        while [[ $k -le ${#node_kinds[@]} ]]; do
                            if [[ $rust_analyzer_idx -eq $parent_idx ]]; then
                                # Found the parent in our arrays
                                if [[ "${node_kinds[$k]}" == "Impl" ]] || [[ "${node_kinds[$k]}" == "Trait" ]]; then
                                    skip=1
                                fi
                                break
                            fi
                            # Only increment rust_analyzer_idx for non-empty nodes (matching parse_rust_analyzer_output logic)
                            if [[ -n "${node_labels[$k]}" ]]; then
                                ((rust_analyzer_idx+=1))
                            fi
                            ((k+=1))
                        done
                    fi

                    if [[ $skip -eq 0 ]]; then
                        if [[ $found_items -eq 0 ]]; then
                            check_relative_path "$file"
                            format_file_header "$file" >> "$OUTPUT_FILE"
                            found_items=1
                        fi

                        local fn_label="${node_labels[$i]}"
                        local fn_line_num=$(get_line_number "${node_ranges[$i]}" "${line_starts[@]}")
                        local fn_module_path=$(build_module_path $i "$file")
                        if [[ -n "${node_details[$i]}" ]]; then
                            echo "- fn $fn_label: $fn_line_num (${node_details[$i]}) [module: $fn_module_path]" >> "$OUTPUT_FILE"
                        else
                            echo "- fn $fn_label: $fn_line_num [module: $fn_module_path]" >> "$OUTPUT_FILE"
                        fi
                    fi
                fi
                ((i+=1))
            done
        else
            # For Struct|Enum, use the simple loop but still process all types
            local i=1
            while [[ $i -le ${#node_kinds[@]} ]]; do
                local kind="${node_kinds[$i]}"
                local matches=0

                case "$symbol_kind" in
                    "Struct|Enum")
                        [[ "$kind" == "Struct" || "$kind" == "Enum" ]] && matches=1
                        ;;
                esac

                if [[ $matches -eq 1 ]]; then
                    if [[ $found_items -eq 0 ]]; then
                        check_relative_path "$file"
                        format_file_header "$file" >> "$OUTPUT_FILE"
                        found_items=1
                    fi

                    local label="${node_labels[$i]}"
                    local line_num=$(get_line_number "${node_ranges[$i]}" "${line_starts[@]}")
                    local struct_module_path=$(build_module_path $i "$file")
                    echo "- $kind $label: $line_num [module: $struct_module_path]" >> "$OUTPUT_FILE"
                fi
                ((i+=1))
            done
        fi

        if [[ $found_items -eq 1 ]]; then
            echo "" >> "$OUTPUT_FILE"
        fi
    done

    echo "  Completed $section_title" >&2
}

# Build module path for a given node index
# Uses the global arrays populated by parse_rust_analyzer_output
# For impl blocks and their contents, includes struct/trait context
build_module_path() {
    local idx=$1
    local file_path=$2
    local -a path_parts=()
    local current_idx="${node_parents[$idx]}"
    local impl_context=""

    # Check if current node is an impl block
    if [[ "${node_kinds[$idx]}" == "Impl" ]]; then
        impl_context=$(extract_impl_context "${node_labels[$idx]}" "header")
    fi

    # Check if this node is inside an impl block (for methods)
    if [[ -z "$impl_context" ]] && [[ -n "$current_idx" ]] && [[ "$current_idx" != "-1" ]]; then
        # Convert rust-analyzer 0-based index to zsh 1-based array position
        local parent_array_idx=$((current_idx + 1))
        if [[ "$current_idx" =~ ^[0-9]+$ ]] && [[ $parent_array_idx -le ${#node_labels[@]} ]]; then
            if [[ "${node_kinds[$parent_array_idx]}" == "Impl" ]]; then
                # This is a method inside an impl block
                impl_context=$(extract_impl_context "${node_labels[$parent_array_idx]}" "method")
            fi
        fi
    fi

    # Traverse up the parent chain collecting module names
    while [[ -n "$current_idx" ]] && [[ "$current_idx" != "-1" ]]; do
        # Convert rust-analyzer 0-based index to zsh 1-based array position
        local parent_array_idx=$((current_idx + 1))
        if [[ "$current_idx" =~ ^[0-9]+$ ]] && [[ $parent_array_idx -le ${#node_labels[@]} ]]; then
            if [[ "${node_kinds[$parent_array_idx]}" == "Module" ]]; then
                path_parts=("${node_labels[$parent_array_idx]}" "${path_parts[@]}")
            fi
            current_idx="${node_parents[$parent_array_idx]}"
        else
            break
        fi
    done

    # Build the base module path
    local base_path
    local file_module=""

    # First, determine the module from the file path
    if [[ "$file_path" =~ (^|/)src/(lib|main)\.rs$ ]]; then
        # lib.rs or main.rs are at crate root
        file_module=""
    elif [[ "$file_path" =~ (^|/)src/(.+)\.rs$ ]]; then
        # Any other file under src/ - extract the path after src/
        local module_from_file="${match[2]}"
        file_module="${module_from_file//\//:}"  # Replace / with ::
    else
        echo "ERROR: Unexpected file path pattern: $file_path" >&2
        exit 1
    fi

    # Now build the full path including both file module and explicit modules
    if [[ ${#path_parts[@]} -eq 0 ]]; then
        # No explicit modules found
        if [[ -z "$file_module" ]]; then
            base_path="crate"
        else
            base_path="crate::${file_module}"
        fi
    else
        # Have explicit modules - need to combine with file module
        local joined="${(j[::])path_parts}"
        if [[ -z "$file_module" ]]; then
            base_path="crate::${joined}"
        else
            base_path="crate::${file_module}::${joined}"
        fi
    fi

    # Add impl context if we have it
    if [[ -n "$impl_context" ]]; then
        echo "${base_path}::${impl_context}"
    else
        echo "${base_path}"
    fi
}

# Extract impl context from an impl label
# context_type: "header" for impl block itself, "method" for functions inside impl
extract_impl_context() {
    local impl_label=$1
    local context_type=$2

    if [[ "$impl_label" =~ ^impl[[:space:]]+([^[:space:]]+)[[:space:]]+for[[:space:]]+([^[:space:]]+) ]]; then
        # Trait implementation: "impl Trait for Type"
        local trait_name="${match[1]}"
        local type_name="${match[2]}"
        if [[ "$context_type" == "header" ]]; then
            # For the impl header itself, just add the type name
            echo "${type_name}"
        else
            # For methods inside, use the full <Type as Trait> syntax
            echo "<${type_name} as ${trait_name}>"
        fi
    elif [[ "$impl_label" =~ ^impl[[:space:]]+([^[:space:]]+) ]]; then
        # Inherent implementation: "impl Type"
        local type_name="${match[1]}"
        echo "${type_name}"
    else
        # No impl context - this is expected for module-level items
        echo ""
    fi
}

# Parse rust-analyzer symbols output into arrays
parse_rust_analyzer_output() {
    local symbols=$1

    # Arrays to return via global variables - clear them first
    node_parents=()
    node_labels=()
    node_kinds=()
    node_ranges=()
    node_details=()
    local node_index=0

    while IFS= read -r line; do
        if [[ -z "$line" ]]; then
            continue
        fi

        # Extract parent info
        local parent_idx=""
        if [[ "$line" =~ 'parent: Some\(([0-9]+)\)' ]]; then
            parent_idx="${match[1]}"  # In zsh, use match array
        elif [[ "$line" =~ "parent: None" ]]; then
            parent_idx="-1"  # Use -1 to indicate no parent
        fi

        local label=$(echo "$line" | grep -oE 'label: "[^"]*"' | cut -d'"' -f2 || true)
        local kind=$(echo "$line" | { grep -oE 'kind: SymbolKind\([^)]*\)' | grep -oE '\([^)]*\)' | tr -d '()' || true; })
        local nav_range=$(echo "$line" | { grep -oE 'navigation_range: [0-9]+\.\.[0-9]+' | grep -oE '[0-9]+' | head -1 || true; })
        local detail=$(echo "$line" | { grep -oE 'detail: Some\("[^"]*"\)' | cut -d'"' -f2 || true; })

        if [[ -n "$label" ]]; then
            node_parents+=("$parent_idx")
            node_labels+=("$label")
            node_kinds+=("$kind")
            node_ranges+=("$nav_range")
            node_details+=("$detail")
            ((node_index+=1))
        fi
    done <<< "$symbols"
}

# Process all traits in the codebase with their method signatures and implementations
process_all_traits_with_details() {
    echo "Processing: Traits..." >&2

    echo "## Traits" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"

    # First pass: collect all traits and their locations
    echo "  Pass 1: Collecting traits..." >&2
    local -A trait_files
    local -A trait_line_nums
    local -A trait_methods  # Store methods for each trait
    local -A trait_module_paths  # Store module paths for each trait
    local file_count=0
    local pass_number=1
    local total_passes=5

    # Determine file list based on single file option
    local -a files_to_process
    if [[ -n "$SINGLE_FILE" ]]; then
        files_to_process=("$SINGLE_FILE")
        echo "  Processing single file: $SINGLE_FILE" >&2
    else
        files_to_process=($(discover_workspace_rust_files))
    fi

    local total_files=${#files_to_process[@]}
    local total_file_scans=$((total_files * total_passes))

    for file in "${files_to_process[@]}"; do
        ((file_count+=1))
        local overall_progress=$(((pass_number - 1) * total_files + file_count))
        echo "  [$overall_progress/$total_file_scans] Scanning $file for traits..." >&2

        # Read file contents (stdin already read at startup)
        if [[ $STDIN_MODE -ne 1 ]]; then
            read_file_to_global "$file"
        fi

        local -a line_starts
        line_starts=($(printf '%s' "$FILE_CONTENTS" | build_line_map_from_stdin))
        local line_map_exit=$?
        if ! command -v rust-analyzer >/dev/null 2>&1; then
            echo "ERROR: rust-analyzer not found in PATH" >&2
            echo "PATH: $PATH" >&2
            exit 1
        fi
        local rust_analyzer_exit=0

        # Temporarily disable set -e for this command
        set +e
        local symbols=$(printf '%s' "$FILE_CONTENTS" | rust-analyzer symbols 2>/dev/null)
        rust_analyzer_exit=$?
        set -e
        if [[ $rust_analyzer_exit -ne 0 ]]; then
            echo "ERROR: rust-analyzer failed with exit code $rust_analyzer_exit" >&2
            echo "ERROR output: $symbols" >&2
            exit 1
        fi

        # Parse symbols
        local -a node_parents node_labels node_kinds node_ranges node_details
        parse_rust_analyzer_output "$symbols"

        # Find traits in this file
        local i=1
        while [[ $i -le ${#node_kinds[@]} ]]; do
            if [[ "${node_kinds[$i]}" == "Trait" ]]; then
                local trait_name="${node_labels[$i]}"
                local trait_line_num=$(get_line_number "${node_ranges[$i]}" "${line_starts[@]}")
                local trait_module_path=$(build_module_path $i "$file")
                trait_files[$trait_name]="$file"
                trait_line_nums[$trait_name]="$trait_line_num"
                trait_module_paths[$trait_name]="$trait_module_path"

                # Collect methods for this trait
                # We need to find the rust-analyzer index for this trait
                # Count how many nodes come before this one (excluding empty ones)
                local rust_analyzer_idx=0
                local k=1
                while [[ $k -lt $i ]]; do
                    if [[ -n "${node_labels[$k]}" ]]; then
                        ((rust_analyzer_idx+=1))
                    fi
                    ((k+=1))
                done

                local methods_text=""
                local j=1
                while [[ $j -le ${#node_parents[@]} ]]; do
                    if [[ "${node_parents[$j]}" == "$rust_analyzer_idx" ]] && [[ "${node_kinds[$j]}" =~ ^(Function|Method)$ ]]; then
                        local method_line_num=$(get_line_number "${node_ranges[$j]}" "${line_starts[@]}")
                        local method_module_path=$(build_module_path $j "$file")
                        if [[ -n "${node_details[$j]}" ]]; then
                            methods_text+="  - fn ${node_labels[$j]}: $method_line_num (${node_details[$j]}) [module: $method_module_path]\n"
                        else
                            methods_text+="  - fn ${node_labels[$j]}: $method_line_num [module: $method_module_path]\n"
                        fi
                    fi
                    ((j+=1))
                done
                trait_methods[$trait_name]="$methods_text"
            fi
            ((i+=1))
        done
    done

    # Second pass: collect all implementations
    echo "  Pass 2: Collecting implementations..." >&2
    local -A trait_implementations
    for trait_name in "${(@k)trait_files}"; do
        trait_implementations[$trait_name]=""
    done

    file_count=0
    pass_number=2
    for file in "${files_to_process[@]}"; do
        ((file_count+=1))
        local overall_progress=$(((pass_number - 1) * total_files + file_count))
        echo "  [$overall_progress/$total_file_scans] Scanning $file for implementations..." >&2

        # Read file contents (stdin already read at startup)
        if [[ $STDIN_MODE -ne 1 ]]; then
            read_file_to_global "$file"
        fi

        local -a line_starts
        local symbols="init string"

        # Always use FILE_CONTENTS
        line_starts=($(printf '%s' "$FILE_CONTENTS" | build_line_map_from_stdin))
        symbols=$(printf '%s' "$FILE_CONTENTS" | rust-analyzer symbols 2>/dev/null)

        # Parse symbols
        local -a node_parents node_labels node_kinds node_ranges node_details
        parse_rust_analyzer_output "$symbols"

        # Find implementations
        local i=1
        while [[ $i -le ${#node_kinds[@]} ]]; do
            if [[ "${node_kinds[$i]}" == "Impl" ]]; then
                local impl_label="${node_labels[$i]}"
                # Check if this implements one of our traits
                for trait_name in "${(@k)trait_files}"; do
                    if [[ "$impl_label" =~ "impl $trait_name for" ]]; then
                        local impl_line_num=$(get_line_number "${node_ranges[$i]}" "${line_starts[@]}")
                        local impl_module_path=$(build_module_path $i "$file")
                        check_relative_path "$file"
                        trait_implementations[$trait_name]+="    - $impl_label: $file:$impl_line_num [module: $impl_module_path]\n"
                    fi
                done
            fi
            ((i+=1))
        done
    done

    # Third pass: output traits with their methods and implementations
    echo "  Pass 3: Writing trait details..." >&2
    for trait_name in "${(@k)trait_files}"; do
        local file="${trait_files[$trait_name]}"
        local trait_line_num="${trait_line_nums[$trait_name]}"

        check_relative_path "$file"
        format_file_header "$file" >> "$OUTPUT_FILE"
        local trait_module_path="${trait_module_paths[$trait_name]}"
        echo "- trait $trait_name: $trait_line_num [module: $trait_module_path]" >> "$OUTPUT_FILE"

        # Output the methods we collected in pass 1
        if [[ -n "${trait_methods[$trait_name]}" ]]; then
            echo -n "${trait_methods[$trait_name]}" >> "$OUTPUT_FILE"
        fi

        # Output implementations
        if [[ -n "${trait_implementations[$trait_name]}" ]]; then
            echo "" >> "$OUTPUT_FILE"
            echo "  - $trait_name Implementations" >> "$OUTPUT_FILE"
            echo "" >> "$OUTPUT_FILE"
            echo -n "${trait_implementations[$trait_name]}" >> "$OUTPUT_FILE"
        fi

        echo "" >> "$OUTPUT_FILE"
    done

    echo "  Completed Traits" >&2
}

# Check if we're in a git repository root
check_git_root() {
    # Check if we're in a git repository at all
    if ! git rev-parse --git-dir >/dev/null 2>&1; then
        echo "Error: Not in a git repository" >&2
        echo "This script must be run from within a git repository" >&2
        return 1
    fi

    # Get the git root directory
    local git_root=$(git rev-parse --show-toplevel 2>/dev/null)
    if [[ -z "$git_root" ]]; then
        echo "Error: Could not determine git root directory" >&2
        return 1
    fi

    # Get the current directory (resolved)
    local current_dir=$(pwd -P)

    # Compare them
    if [[ "$current_dir" != "$git_root" ]]; then
        echo "Error: Not at git repository root" >&2
        echo "  Current directory: $current_dir" >&2
        echo "  Git root: $git_root" >&2
        echo "Please run this script from the git repository root" >&2
        return 1
    fi

    return 0
}

# Main script
main() {
    echo "Starting code map generation..." >&2

    # Change to project root for all operations
    cd "$PROJECT_ROOT" || {
        echo "Error: Cannot change to project root: $PROJECT_ROOT" >&2
        exit 1
    }

    # Verify we're at git root
    check_git_root || {
        exit 1
    }

    if [[ $OUTPUT_TO_STDOUT -eq 0 ]]; then
        mkdir -p $PROJECT_ROOT/.agentic-shepherd
        echo "Writing header to $OUTPUT_FILE..." >&2
    fi

    # Start output file
    cat > "$OUTPUT_FILE" << 'HEADER'
# Code Map

## Generation

This file is auto-generated. To refresh:
```bash
./bin/code-map-refresh.zsh
```

HEADER

    echo "Generated on: $(date)" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"

    # Build file to crate mapping
    echo "Building file to crate mapping..." >&2
    build_file_to_crate_mapping

    # Process different symbol types
    process_all_traits_with_details
    process_symbols "Structs and Enums" "Struct|Enum" 3
    process_symbols "Module Functions" "Function" 4
    process_symbols "All Implementations" "Impl" 5

    echo "Code map generated in $OUTPUT_FILE" >&2
    echo "Done!" >&2
}

# Run main function
main "$@"
