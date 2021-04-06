import subprocess


def get_host_ip():
    host_ip = subprocess.Popen(['ifconfig | grep -e "inet\s" | awk \'NR==1{print $2}\''], stdout=subprocess.PIPE,
                               shell=True)
    (IP, errors) = host_ip.communicate()
    host_ip.stdout.close()
    ip_string = IP.decode('utf-8').strip("\n")
    return ip_string
