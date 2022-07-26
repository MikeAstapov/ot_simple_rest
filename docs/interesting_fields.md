# Interesting Fields

### Example: 
GET http://localhost:50000/api/getinterestingfields?cid=170&from=1653811804&to=1655194188

If **from** not specified, the data won't be sliced from the left
If **to** not specified, the data won't be sliced from the right

**from** and **to** are passed as unix timestamp

### Note!
The data returned includes both borders (**from** and **to**)