     public class IntArrayWritable extends ArrayWritable {
                        public IntArrayWritable() { super(IntWritable.class); }
                        public IntArrayWritable(IntWritable[] values) {
                                super(IntWritable.class, values);
                        }
                }

