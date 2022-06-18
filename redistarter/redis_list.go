package redistarter

import (
	"errors"

	"github.com/gomodule/redigo/redis"
)

type EmptyListError struct{}

func (emp EmptyListError) Error() string {
	return "redis list is empty"
}

func Rpush(listkey string, item string) error {
	if Pool == nil {
		return errors.New("GetStrval(): redis client pool is not initiallized")
	}

	conn := Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	_, err := conn.Do("RPUSH", listkey, item)
	return err
}

func Lpush(listkey string, item string) error {
	if Pool == nil {
		return errors.New("GetStrval(): redis client pool is not initiallized")
	}

	conn := Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	_, err := conn.Do("LPUSH", listkey, item)
	return err
}

func Lpop(listkey string) (string, error) {
	if Pool == nil {
		return "", errors.New("GetStrval(): redis client pool is not initiallized")
	}

	conn := Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return "", err
	}
	data, err := conn.Do("LPOP", listkey)
	if err != nil {
		return "", err
	}
	if data == nil {
		return "", EmptyListError{} // list is empty
	}
	b, err := redis.Bytes(data, err)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func Rpop(listkey string) (string, error) {
	if Pool == nil {
		return "", errors.New("GetStrval(): redis client pool is not initiallized")
	}

	conn := Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return "", err
	}
	data, err := conn.Do("RPOP", listkey)
	if err != nil {
		return "", err
	}
	if data == nil {
		return "", nil // no data was associated with this key
	}
	b, err := redis.Bytes(data, err)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func Llen(listkey string) (int64, error) {
	if Pool == nil {
		return 0, errors.New("GetStrval(): redis client pool is not initiallized")
	}

	conn := Pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return 0, err
	}
	num, err := conn.Do("LLEN", listkey)
	if err != nil {
		return 0, err
	}
	return num.(int64), nil
}
