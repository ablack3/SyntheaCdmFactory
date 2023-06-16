# curl https://api.github.com/repos/{organisation}/{repository}/contents/{file or folder path}
system('curl https://api.github.com/repos/OdyOSG/SyntheaCdmFactory/contents/vocab/vocabulary_bundle_v5_0-22-JUN-22.zip')


# json query
# {
#     "operation": "download",
#     "transfer": ["basic"],
#     "objects": [
#         {"oid": "cef1a979b260b31483a3c90e4319cc42ec8345c68878c05f6ab47bc031e145da", "size": "894591721"}
#     ]}
# }



# curl -X POST \
# -H "Accept: application/vnd.git-lfs+json" \
# -H "Content-type: application/json" \
# -d '{"operation": "download", "transfer": ["basic"], "objects": [{"oid": "cef1a979b260b31483a3c90e4319cc42ec8345c68878c05f6ab47bc031e145da", "size": 894591721}]}' \
# https://github.com/OdyOSG/SyntheaCdmFactory.git/info/lfs/objects/batch


"https://github-cloud.githubusercontent.com/alambic/media/561076885/ce/f1/cef1a979b260b31483a3c90e4319cc42ec8345c68878c05f6ab47bc031e145da?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIMWPLRQEC4XCWWPA%2F20230312%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20230312T094103Z&X-Amz-Expires=3600&X-Amz-Signature=c66b716381a032a16e54eac2a8b8367738a3e0d1dcb5ef84d9c3292a1c9aa6a3&X-Amz-SignedHeaders=host&actor_id=0&key_id=0&repo_id=612683306&token=1"
