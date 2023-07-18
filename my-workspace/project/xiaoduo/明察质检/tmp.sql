        // exclude boundary condition
        List<List<Integer>> res = new ArrayList<>();
        if (nums == null || nums.length < 3) {
            return res;
        }

        // handle
        Arrays.sort(nums);
        for (int i = 0; i < nums.length - 2; i++) {
            int target = -nums[i];
            int left = i + 1, right = nums.length - 1;

            while (left < right) {
                if (nums[left] + nums[right] < target) {
                    left += 1;
                }
                else if (nums[left] + nums[right] > target) {
                    right -= 1;
                }
                else {
                    List<Integer> tuple = new ArrayList<>();
                    tuple.add(nums[i]);
                    tuple.add(nums[left]);
                    tuple.add(nums[right]);
                    res.add(tuple);

                    // there might be multiple tuples
                    left += 1;
                    // skip duplicate elements in second position to avoid duplicate tuple
                    while (left < right && nums[left - 1] == nums[left]) {
                        left += 1;
                    }
                }
            }

            // skip duplicate elements in first position to avoid duplicate tuple
            while (i + 1 < nums.length - 2 && nums[i] == nums[i + 1]) {
                i += 1;
            }
        }

        // return
        return res;
    }