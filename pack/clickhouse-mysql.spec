%define name clickhouse-mysql
%define version 0.0.20180227
%define release 1

Summary: MySQL to ClickHouse data migrator
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{version}.tar.gz
License: MIT
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Altinity (Vladislav Klimenko <sunsingerus@gmail.com>)
Packager: Altinity (Vladislav Klimenko <sunsingerus@gmail.com>)
Url: https://github.com/altinity/clickhouse-mysql-data-reader

%description
MySQL to ClickHouse data migrator

%prep
set -x
%setup -n %{name}-%{version} -n %{name}-%{version}

%build
set -x
python3 setup.py build

%install
set -x
#python3 setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES
python3 setup.py install --single-version-externally-managed -O1 --root=%{buildroot} --record=INSTALLED_FILES --prefix=/usr

mkdir -p %{buildroot}/etc/clickhouse-mysql
mkdir -p %{buildroot}/etc/init.d
mkdir -p %{buildroot}/etc/systemd/system

cp %{_builddir}/%{buildsubdir}/clickhouse-mysql.conf           %{buildroot}/etc/clickhouse-mysql/clickhouse-mysql-example.conf
cp %{_builddir}/%{buildsubdir}/init.d/clickhouse-mysql         %{buildroot}/etc/init.d/clickhouse-mysql
cp %{_builddir}/%{buildsubdir}/init.d/clickhouse-mysql.service %{buildroot}/etc/systemd/system/clickhouse-mysql.service

%clean
rm -rf %{buildroot}

%files -f INSTALLED_FILES
/etc/clickhouse-mysql/clickhouse-mysql-example.conf
/etc/init.d/clickhouse-mysql
/etc/systemd/system/clickhouse-mysql.service
%defattr(-,root,root)

