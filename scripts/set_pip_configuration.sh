pypi_host="pypi.python.org"
base_pyindex="https://$pypi_host/simple"
echo "[global]" > $1/pip.conf
echo "index = $base_pyindex" >> $1/pip.conf
echo "trusted-host = $pypi_host" >> $1/pip.conf

# To add a local package repository, it can be configured as follows:
# local_repo_host="some.local.repo"
# local_repo_path="http://$local_repo_host/simple/
# local_repo_pyindex="https://$local_repo_host/repository/erudition-pypi/simple"
# echo "[global]" > $1/pip.conf
# echo "index = $local_repo_pyindex" >> $1/pip.conf
# echo "index-url = $local_repo_pyindex" >> $1/pip.conf
# echo "extra-index-url = $base_pyindex" >> $1/pip.conf
# echo "trusted-host = $local_repo_host $pypi_host" >> $1/pip.conf
