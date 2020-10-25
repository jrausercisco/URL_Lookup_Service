# URL Lookup Service

## Instructions

From root, run the following command

```
docker build -t url_lookup_image .
docker run -d --name url_lookup_container -p 8000:80 url_lookup_image
```

For swagger docs, traverse to http://127.0.0.1:8000/docs#
