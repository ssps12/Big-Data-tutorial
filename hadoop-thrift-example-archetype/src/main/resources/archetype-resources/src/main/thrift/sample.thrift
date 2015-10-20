namespace java edu.uchicago.mpcs53013

# A few simple thrift classes

enum StudentType {
  COLLEGE = 1,
  MPCS = 2,
  HARRIS = 3,
  DOCTORAL = 4
 }
 
struct Student {
  1: required string name;
  2: required i32 homeworkTotal;
  3: required StudentType type;
 }